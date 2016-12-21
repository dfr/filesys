/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <rpc++/cred.h>
#include <glog/logging.h>

#include "client.h"
#include "session.h"
#include "util.h"

using namespace filesys::nfs4;
using namespace nfsd::nfs4;
using namespace std;

NfsSession::NfsSession(
    shared_ptr<NfsClient> client,
    shared_ptr<oncrpc::Channel> chan,
    const channel_attrs4& fca, const channel_attrs4& bca,
    uint32_t cbProg, const vector<callback_sec_parms4>& cbSec)
    : client_(client),
      id_(client->newSessionId())
{
    channels_.push_back(chan);
    backChannel_ = chan;
    slots_.resize(fca.ca_maxrequests);
    cbSlots_.resize(bca.ca_maxrequests);
    cbHighestSlot_ = 0;
    targetCbHighestSlot_ = bca.ca_maxrequests;
    setCallback(cbProg, cbSec);
}

void NfsSession::setCallback(
    uint32_t cbProg, const vector<callback_sec_parms4>& cbSec)
{
    cbClient_.reset();
    for (auto& sec: cbSec) {
        switch (sec.cb_secflavor) {
        case AUTH_NONE:
            cbClient_ = make_shared<oncrpc::Client>(cbProg, NFS_CB);
            break;
        case AUTH_SYS: {
            vector<int32_t> gids = sec.cbsp_sys_cred().gids;
            oncrpc::Credential cred(
                sec.cbsp_sys_cred().uid,
                sec.cbsp_sys_cred().gid,
                move(gids), false);
            auto client = make_shared<oncrpc::SysClient>(cbProg, NFS_CB);
            client->set(cred);
            cbClient_ = client;
            break;
        }
        default:
            break;
        }
        if (cbClient_)
            break;
    }
    if (!cbClient_) {
        LOG(ERROR) << "Can't set security parameters for back channel";
        backChannel_.reset();
    }
}

SEQUENCE4res NfsSession::sequence(
    CompoundState& state, const SEQUENCE4args& args)
{
    unique_lock<mutex> lock(mutex_);

    auto client = client_.lock();
    if (client->spa() != SP4_NONE) {
        // Check that the channel is bound to this session
        auto chan = oncrpc::CallContext::current().channel();
        bool valid = false;
        for (auto p: channels_) {
            if (p.lock() == chan) {
                valid = true;
                break;
            }
        }
        LOG(ERROR) << "Request received on channel not bound to session";
        return SEQUENCE4res(NFS4ERR_CONN_NOT_BOUND_TO_SESSION);
    }

    auto slotp = slots_.data() + args.sa_slotid;
    if (args.sa_slotid >= slots_.size())
        return SEQUENCE4res(NFS4ERR_BADSLOT);
    else if (args.sa_highest_slotid >= slots_.size())
        return SEQUENCE4res(NFS4ERR_BAD_HIGH_SLOT);
    else if (args.sa_sequenceid < slotp->sequence ||
             args.sa_sequenceid > slotp->sequence + 1)
        return SEQUENCE4res(NFS4ERR_SEQ_MISORDERED);
    else if (slotp->busy)
        return SEQUENCE4res(NFS4ERR_DELAY);
    else {
        uint32_t flags = 0;

        if (client_.lock()->hasRevokedState()) {
            flags |= SEQ4_STATUS_EXPIRED_SOME_STATE_REVOKED;
        }
        if (backChannelState_ == NONE) {
            flags |= SEQ4_STATUS_CB_PATH_DOWN_SESSION;
        }

        slotp->busy = true;
        state.slot = slotp;
        state.session = shared_from_this();
        return SEQUENCE4res(
            NFS4_OK, SEQUENCE4resok{
                args.sa_sessionid,
                slotp->sequence + 1,
                args.sa_slotid,
                args.sa_highest_slotid,
                slotid4(slots_.size() - 1),
                flags});
    }
}

void NfsSession::addChannel(
    shared_ptr<oncrpc::Channel> chan, int dir, bool use_rdma)
{
    unique_lock<mutex> lk(mutex_);

    bool addChan = (dir & CDFC4_FORE) != 0;
    vector<weak_ptr<oncrpc::Channel>> newChannels;
    for (auto p: channels_) {
        auto oldChan = p.lock();
        if (oldChan) {
            newChannels.push_back(oldChan);
            if (chan == oldChan)
                addChan = false;
        }
    }
    if (addChan)
        newChannels.push_back(chan);
    channels_ = move(newChannels);
    if (dir & CDFC4_BACK) {
        // We need to test the new back channel
        backChannel_ = chan;
        backChannelState_ = UNCHECKED;
    }
}

bool NfsSession::validChannel(shared_ptr<oncrpc::Channel> chan)
{
    unique_lock<mutex> lock(mutex_);
    for (auto p: channels_) {
        if (p.lock() == chan)
            return true;
    }
    return false;
}

void NfsSession::callback(
    const std::string& tag,
    std::function<void(filesys::nfs4::CallbackRequestEncoder& enc)> args,
    std::function<void(filesys::nfs4::CallbackReplyDecoder& dec)> res)
{
    auto chan = backChannel_.lock();
    if (!chan) {
        throw filesys::nfs4::NFS4ERR_CB_PATH_DOWN;
    }
    for (;;) {
        std::unique_lock<std::mutex> lk(mutex_);
        int slot = -1, newHighestSlot;
        while (slot == -1) {
            int limit = std::min(
                int(cbSlots_.size()) - 1, cbHighestSlot_ + 1);
            for (int i = 0; i <= limit; i++) {
                auto& s = cbSlots_[i];
                if (s.busy) {
                    newHighestSlot = i;
                }
                else if (i <= targetCbHighestSlot_ && slot == -1) {
                    slot = i;
                    newHighestSlot = i;
                }
            }
            if (slot == -1) {
                cbSlotWait_.wait(lk);
                continue;
            }
            cbHighestSlot_ = newHighestSlot;
        }
        auto p = std::unique_ptr<CBSlot, std::function<void(CBSlot*)>>(
            &cbSlots_[slot],
            [this](CBSlot* p) {
                p->busy = false;
                cbSlotWait_.notify_one();
            });
        p->busy = true;
        filesys::nfs4::sequenceid4 seq = p->sequence++;
        VLOG(2) << "CB slot: " << slot
                << ", highestSlot: " << cbHighestSlot_
                << ", sequence: " << seq;
        lk.unlock();

        int newTarget;
        bool revoked = false;
        try {
            chan->call(
                cbClient_.get(), filesys::nfs4::CB_COMPOUND,
                [&tag, &args, slot, seq, this](auto xdrs) {
                    filesys::nfs4::CallbackRequestEncoder enc(tag, xdrs);
                    enc.sequence(
                        id_, seq, slot, cbHighestSlot_, false, {});
                    args(enc);
                },
                [&tag, &res, &newTarget, &revoked, this](auto xdrs) {
                    filesys::nfs4::CallbackReplyDecoder dec(tag, xdrs);
                    auto seqres = dec.sequence();
                    newTarget = seqres.csr_target_highest_slotid;
                    res(dec);
                });
        }
        catch (nfsstat4 stat) {
            if (stat == NFS4ERR_BADSESSION) {
                LOG(ERROR) << "Backchannel request failed, status: " << stat;
                lk.lock();
                backChannel_.reset();
                backChannelState_ = NONE;
                return;
            }
            else {
                throw;
            }
        }
        catch (system_error& e) {
            LOG(ERROR) << "Backchannel request failed: " << e.what();
            lk.lock();
            backChannel_.reset();
            backChannelState_ = NONE;
            return;
        }
        catch (oncrpc::RpcError& e) {
            LOG(ERROR) << "Backchannel request failed: " << e.what();
            lk.lock();
            backChannel_.reset();
            backChannelState_ = NONE;
            return;
        }

        lk.lock();
        backChannelState_ = GOOD;

        p.reset();
        if (newTarget != targetCbHighestSlot_) {
            if (cbSlots_.size() < newTarget + 1)
                cbSlots_.resize(newTarget + 1);
            targetCbHighestSlot_ = newTarget;
        }
        return;
    }
}

bool NfsSession::testBackchannel()
{
    std::unique_lock<std::mutex> lk(mutex_);
    for (;;) {
        auto chan = backChannel_.lock();
        if (!chan) {
            backChannel_.reset();
            backChannelState_ = NONE;
            return false;
        }
        switch (backChannelState_) {
        case NONE:
            return false;

        case UNCHECKED:
            backChannelState_ = CHECKING;
            lk.unlock();
            callback("Test", [](auto&){}, [](auto&){});
            lk.lock();
            backChannelWait_.notify_all();
            return backChannelState_ == GOOD;

        case CHECKING:
            backChannelWait_.wait(lk);
            continue;

        case GOOD:
            return true;
        }
    }
}

void NfsSession::recallDelegation(
    const filesys::nfs4::stateid4& stateid,
    const filesys::nfs4::nfs_fh4& fh)
{
    if (recallHook_) {
        recallHook_(stateid, fh);
        return;
    }
    callback(
        "Recall",
        [&stateid, &fh](auto& enc) {
            enc.recall(stateid, false, fh);
        },
        [](auto& dec) {
            dec.recall();
        });
}

void NfsSession::recallLayout(
    filesys::nfs4::layouttype4 type,
    filesys::nfs4::layoutiomode4 iomode,
    bool changed,
    const filesys::nfs4::layoutrecall4& recall)
{
    if (layoutRecallHook_) {
        layoutRecallHook_(type, iomode, changed, recall);
        return;
    }
    callback(
        "Recall",
        [type, iomode, changed, &recall](auto& enc) {
            enc.layoutrecall(type, iomode, changed, recall);
        },
        [](auto& dec) {
            dec.layoutrecall();
        });
}

bool NfsSession::get(
    std::shared_ptr<oncrpc::RestRequest> req,
    std::unique_ptr<oncrpc::RestEncoder>&& res)
{
    auto enc = res->object();
    auto channels = enc->field("channels")->array();
    for (auto entry: channels_) {
        auto chan = entry.lock();
        if (!chan)
            continue;
        auto addr = chan->remoteAddress();
        channels->element()->string(
            addr.host() + ":" + to_string(addr.port()));
    }
    channels.reset();
    enc.reset();
    return true;
}

void NfsSession::setRestRegistry(
    std::shared_ptr<oncrpc::RestRegistry> restreg)
{
    assert(!restreg_.lock());
    restreg_ = restreg;
    restreg->add(
        std::string("/nfs4/session/") + toHexSessionid(id_), true,
        shared_from_this());
}
