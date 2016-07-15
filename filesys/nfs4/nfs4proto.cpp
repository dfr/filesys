/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <cassert>
//#include <iomanip>
#include <random>
#include <system_error>
#include <unistd.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "nfs4fs.h"

using namespace filesys;
using namespace filesys::nfs4;

static std::random_device rnd;

NfsProto::NfsProto(
    NfsFilesystem* fs,
    std::shared_ptr<oncrpc::Channel> chan,
    std::shared_ptr<oncrpc::Client> client,
    const std::string& clientowner,
    std::uint32_t cbprog)
    : fs_(fs),
      chan_(chan),
      client_(client),
      cbprog_(cbprog)
{
    // First we need to create a new session
    for (auto& b: clientOwner_.co_verifier)
        b = rnd();
    clientOwner_.co_ownerid.resize(clientowner.size());
    std::copy_n(clientowner.data(), clientowner.size(),
           clientOwner_.co_ownerid.data());
    VLOG(1) << "Using client owner " << clientOwner_;
    connect();
}

NfsProto::~NfsProto()
{
    if (chan_)
        disconnect();
}

void NfsProto::compoundNoSequence(
    const std::string& tag,
    std::function<void(CompoundRequestEncoder&)> args,
    std::function<void(CompoundReplyDecoder&)> res)
{
    chan_->call(
        client_.get(), NFSPROC4_COMPOUND,
        [&tag, args, this](auto xdrs) {
            CompoundRequestEncoder enc(tag, xdrs);
            args(enc);
        },
        [&tag, res, this](auto xdrs) {
            CompoundReplyDecoder dec(tag, xdrs);
            res(dec);
        });
}

void NfsProto::compound(
    const std::string& tag,
    std::function<void(CompoundRequestEncoder&)> args,
    std::function<void(CompoundReplyDecoder&)> res)
{
    for (;;) {
        try {
            std::unique_lock<std::mutex> lock(mutex_);
            int slot = -1, newHighestSlot;
            while (slot == -1) {
                int limit = std::min(
                    int(slots_.size()) - 1, highestSlot_ + 1);
                for (int i = 0; i <= limit; i++) {
                    auto& s = slots_[i];
                    if (s.busy_) {
                        newHighestSlot = i;
                    }
                    else if (i <= targetHighestSlot_ && slot == -1) {
                        slot = i;
                        newHighestSlot = i;
                    }
                }
                if (slot == -1) {
                    slotWait_.wait(lock);
                    continue;
                }
                highestSlot_ = newHighestSlot;
            }
            auto p = std::unique_ptr<Slot, std::function<void(Slot*)>>(
                &slots_[slot],
                [this](Slot* p) {
                    p->busy_ = false;
                    slotWait_.notify_one();
                });
            p->busy_ = true;
            sequenceid4 seq = p->sequence_++;
            VLOG(2) << "slot: " << slot
                    << ", highestSlot: " << highestSlot_
                    << ", sequence: " << seq;
            lock.unlock();

            int newTarget;
            bool revoked = false;
            chan_->call(
                client_.get(), NFSPROC4_COMPOUND,
                [&tag, &args, slot, seq, this](auto xdrs) {
                    CompoundRequestEncoder enc(tag, xdrs);
                    enc.sequence(
                        sessionid_, seq, slot, highestSlot_, false);
                    args(enc);
                },
                [&tag, &res, &newTarget, &revoked, this](auto xdrs) {
                    CompoundReplyDecoder dec(tag, xdrs);
                    auto seqres = dec.sequence();
                    newTarget = seqres.sr_target_highest_slotid;
                    constexpr int revflags =
                        SEQ4_STATUS_EXPIRED_ALL_STATE_REVOKED +
                        SEQ4_STATUS_EXPIRED_SOME_STATE_REVOKED +
                        SEQ4_STATUS_ADMIN_STATE_REVOKED +
                        SEQ4_STATUS_RECALLABLE_STATE_REVOKED;
                    if (seqres.sr_status_flags & revflags)
                        revoked = true;
                    res(dec);
                });

            p.reset();
            if (newTarget != targetHighestSlot_) {
                lock.lock();
                if (int(slots_.size()) < newTarget + 1)
                    slots_.resize(newTarget + 1);
                targetHighestSlot_ = newTarget;
                lock.unlock();
            }
            if (revoked) {
                fs_->freeRevokedState();
            }
            return;
        }
        catch (nfsstat4 st) {
            using namespace std::literals;
            switch (st) {
            case NFS4ERR_DELAY:
                std::this_thread::sleep_for(1ms);
                continue;
            case NFS4ERR_GRACE:
                std::this_thread::sleep_for(5s);
                continue;
            case NFS4ERR_BADSESSION:
            case NFS4ERR_DEADSESSION:
                connect();
                continue;
            default:
                throw mapStatus(st);
            }
        }
    }
}

void NfsProto::connect()
{
    bool recovery = false;

retry:
    if (!clientid_) {
        compoundNoSequence(
            "exchange_id",
            [this](auto& enc)
            {
                enc.exchange_id(
                    clientOwner_,
                    (EXCHGID4_FLAG_USE_NON_PNFS |
                     EXCHGID4_FLAG_USE_PNFS_MDS |
                     EXCHGID4_FLAG_USE_PNFS_DS),
                    state_protect4_a(SP4_NONE), {});
            },
            [this](auto& dec)
            {
                auto resok = dec.exchange_id();
                clientid_ = resok.eir_clientid;
                sequence_ = resok.eir_sequenceid;
                if (resok.eir_server_impl_id.size() > 0) {
                    auto& iid = resok.eir_server_impl_id[0];
                    VLOG(1) << "Server implementation ID: "
                            << "domain: " << toString(iid.nii_domain)
                            << ", name: " << toString(iid.nii_name);
                }
            });

        LOG(INFO) << "clientid: " << std::hex << clientid_
                  << ", sequence: " << sequence_;
    }

    try {
        auto slotCount = std::thread::hardware_concurrency();

        compoundNoSequence(
            "create_session",
            [this, slotCount](auto& enc)
            {
                std::vector<callback_sec_parms4> sec_parms;
                sec_parms.emplace_back(AUTH_NONE);
                count4 iosize = 65536 + 512;
                enc.create_session(
                    clientid_, sequence_,
                    CREATE_SESSION4_FLAG_CONN_BACK_CHAN,
                    channel_attrs4{
                        0, iosize, iosize, iosize, 32, slotCount, {}},
                    channel_attrs4{
                        0, iosize, iosize, iosize, 32, slotCount, {}},
                    cbprog_,
                    sec_parms);
            },
            [this](auto& dec)
            {
                auto resok = dec.create_session();
                sessionid_ = resok.csr_sessionid;
                highestSlot_ = 0;
                targetHighestSlot_ = resok.csr_fore_chan_attrs.ca_maxrequests;
                slots_.clear();
                slots_.resize(targetHighestSlot_);
            });
        sequence_++;

        LOG(INFO) << "sessionid: " << sessionid_;
        fs_->setSlots(slotCount);
    }
    catch (nfsstat4 stat) {
        if (stat == NFS4ERR_STALE_CLIENTID) {
            // We need to create a new client and recover state
            clientid_ = 0;
            recovery = true;
            goto retry;
        }
    }

    if (recovery) {
        fs_->recover();
    }
}

void NfsProto::disconnect()
{
    std::unique_lock<std::mutex> lock(mutex_);

    // Destroy the session and client
    try {
        compoundNoSequence(
            "destroy_session",
            [this](auto& enc)
            {
                enc.destroy_session(sessionid_);
            },
            [](auto& dec)
            {
                dec.destroy_session();
            });
        compoundNoSequence(
            "destroy_clientid",
            [this](auto& enc)
            {
                enc.destroy_clientid(clientid_);
            },
            [](auto& dec)
            {
                dec.destroy_clientid();
            });
    }
    catch (nfsstat4 stat) {
        // Suppress errors from expired client state
        if (stat != NFS4ERR_BADSESSION)
            throw;
    }
    catch (std::system_error&) {
    }
    chan_.reset();
}
