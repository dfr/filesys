/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <cassert>
#include <iomanip>
#include <random>
#include <system_error>
#include <unistd.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fs++/urlparser.h>

#include "nfs4fs.h"

using namespace filesys;
using namespace filesys::nfs4;
using namespace std;

DEFINE_string(clientowner, "", "Override default client owner");

static random_device rnd;

NfsFilesystem::NfsFilesystem(
    shared_ptr<oncrpc::Channel> chan,
    shared_ptr<oncrpc::Client> client,
    shared_ptr<detail::Clock> clock,
    const string& clientowner,
    shared_ptr<IIdMapper> idmapper)
    : chan_(chan),
      client_(client),
      clock_(clock),
      idmapper_(idmapper),
      tag_("fscli"),
      sockman_(make_shared<oncrpc::SocketManager>()),
      svcreg_(chan->serviceRegistry()),
      cbsvc_(this)
{
    // Only allow a fraction of the file cache to have active delegations
    delegations_.setSizeLimit(cache_.sizeLimit() / 8);

    // Handle callbacks on a new thread
    if (!svcreg_) {
        svcreg_ = make_shared<oncrpc::ServiceRegistry>();
        chan->setServiceRegistry(svcreg_);
    }

    // Allocate a program number from the transient range
    cbprog_ = svcreg_->allocate(0x40000000, 0x5fffffff);
    cbsvc_.bind(cbprog_, svcreg_);
    cbthread_ = thread([this, chan](){ handleCallbacks(); });

    // First we need to create a new session
    for (auto& b: clientOwner_.co_verifier)
        b = rnd();
    clientOwner_.co_ownerid.resize(clientowner.size());
    copy_n(clientowner.data(), clientowner.size(),
           clientOwner_.co_ownerid.data());
    VLOG(1) << "Using client owner " << clientOwner_;
    connect();
}

NfsFilesystem::NfsFilesystem(
    shared_ptr<oncrpc::Channel> chan,
    shared_ptr<oncrpc::Client> client,
    shared_ptr<detail::Clock> clock,
    const string& clientowner)
    : NfsFilesystem(chan, client, clock, clientowner, LocalIdMapper())
{
}

NfsFilesystem::~NfsFilesystem()
{
    if (chan_)
        disconnect();

    sockman_->stop();
    cbsvc_.unbind(cbprog_, svcreg_);
    chan_.reset();
    cbthread_.join();
}

shared_ptr<File>
NfsFilesystem::root()
{
    if (!root_) {
        nfs_fh4 rootfh;
        fattr4 rootattr;
        compound(
            [](auto& enc)
            {
                bitmap4 wanted;
                setSupportedAttrs(wanted);
                set(wanted, FATTR4_MAXREAD);
                set(wanted, FATTR4_MAXWRITE);
                enc.putrootfh();
                enc.getattr(wanted);
                enc.getfh();
            },
            [&rootfh, &rootattr](auto& dec)
            {
                dec.putrootfh();
                rootattr = move(dec.getattr().obj_attributes);
                rootfh = move(dec.getfh().object);
            });

        root_ = find(move(rootfh), move(rootattr));

        fsinfo_.maxread = root_->attr().maxread_;
        fsinfo_.maxwrite = root_->attr().maxwrite_;

        // Set the buffer size for the largest read or write request we will
        // make, allowing extra space for protocol overhead
        chan_->setBufferSize(512 + max(fsinfo_.maxread, fsinfo_.maxwrite));
    }
    return root_;
}

const FilesystemId&
NfsFilesystem::fsid() const
{
    static FilesystemId nullfsid;
    return nullfsid;
}

shared_ptr<File>
NfsFilesystem::find(const FileHandle& fh)
{
    throw system_error(ESTALE, system_category());
}

void
NfsFilesystem::unmount()
{
    // Clear our delegation cache to return the delegations back to
    // the server
    delegations_.clear();

    // XXX we should keep track of NfsOpenFile instances and
    // force-close them here

    disconnect();
}

void
NfsFilesystem::compoundNoSequence(
    function<void(CompoundRequestEncoder&)> args,
    function<void(CompoundReplyDecoder&)> res)
{
    chan_->call(
        client_.get(), NFSPROC4_COMPOUND,
        [args, this](auto xdrs) {
            CompoundRequestEncoder enc(tag_, xdrs);
            args(enc);
        },
        [res, this](auto xdrs) {
            CompoundReplyDecoder dec(tag_, xdrs);
            res(dec);
        });
}

void
NfsFilesystem::compound(
    function<void(CompoundRequestEncoder&)> args,
    function<void(CompoundReplyDecoder&)> res)
{
    for (;;) {
        try {
            unique_lock<mutex> lock(mutex_);
            int slot = -1, newHighestSlot;
            while (slot == -1) {
                int limit = min(
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
            auto p = unique_ptr<Slot, function<void(Slot*)>>(
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
                [&args, slot, seq, this](auto xdrs) {
                    CompoundRequestEncoder enc(tag_, xdrs);
                    enc.sequence(
                        sessionid_, seq, slot, highestSlot_, false);
                    args(enc);
                },
                [&res, &newTarget, &revoked, this](auto xdrs) {
                    CompoundReplyDecoder dec(tag_, xdrs);
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
            if (revoked)
                freeRevokedState();
            return;
        }
        catch (nfsstat4 st) {
            using namespace std::literals;
            switch (st) {
            case NFS4ERR_DELAY:
                this_thread::sleep_for(1ms);
                continue;
            case NFS4ERR_GRACE:
                this_thread::sleep_for(5s);
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

shared_ptr<NfsFile>
NfsFilesystem::find(const nfs_fh4& fh)
{
    unique_lock<mutex> lock(mutex_);
    return cache_.find(
        fh,
        [&](auto file) {
        },
        [&](nfs_fh4 id) {
            return make_shared<NfsFile>(
                shared_from_this(), move(id));
        });
}

shared_ptr<NfsFile>
NfsFilesystem::find(nfs_fh4&& fh, fattr4&& attr)
{
    unique_lock<mutex> lock(mutex_);
    return cache_.find(
        fh,
        [&](auto file) {
            file->update(move(attr));
        },
        [&](const nfs_fh4& id) {
            return make_shared<NfsFile>(
                shared_from_this(), move(fh), move(attr));
        });
}

shared_ptr<NfsDelegation>
NfsFilesystem::addDelegation(
    shared_ptr<NfsFile> file, shared_ptr<NfsOpenFile> of,
    open_delegation4&& delegation)
{
    unique_lock<mutex> lock(mutex_);
    stateid4 stateid;
    switch (delegation.delegation_type) {
    case OPEN_DELEGATE_NONE:
    case OPEN_DELEGATE_NONE_EXT:
        return nullptr;

    case OPEN_DELEGATE_READ:
        stateid = delegation.read().stateid;
        break;

    case OPEN_DELEGATE_WRITE:
        stateid = delegation.write().stateid;
        break;
    }
    auto d = make_shared<NfsDelegation>(file, of, move(delegation));
    delegations_.add(stateid, d);
    return d;
}

void
NfsFilesystem::connect()
{
    bool recovery = false;

retry:
    if (!clientid_) {
        compoundNoSequence(
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
            });

        LOG(INFO) << "clientid: " << hex << clientid_
                  << ", sequence: " << sequence_;
    }

    try {
        auto slotCount = thread::hardware_concurrency();

        compoundNoSequence(
            [this, slotCount](auto& enc)
            {
                vector<callback_sec_parms4> sec_parms;
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
        cbsvc_.setSlots(slotCount);
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
        LOG(INFO) << "recovering state";
        auto lock = cache_.lock();
        for (auto& e: cache_) {
            e.second->recover();
        }
    }
}

void
NfsFilesystem::disconnect()
{
    unique_lock<mutex> lock(mutex_);

    // Destroy the session and client
    try {
        compoundNoSequence(
            [this](auto& enc)
            {
                enc.destroy_session(sessionid_);
            },
            [](auto& dec)
            {
                dec.destroy_session();
            });
        compoundNoSequence(
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
    catch (system_error&) {
    }
    chan_.reset();
}

void
NfsFilesystem::freeRevokedState()
{
    auto lock = cache_.lock(try_to_lock);
    if (lock) {
        LOG(INFO) << "freeing revoked state";
        for (auto& e: cache_) {
            e.second->testState();
        }
    }
}

void
NfsFilesystem::handleCallbacks()
{
    auto sockchan = dynamic_pointer_cast<oncrpc::Socket>(chan_);
    if (sockchan) {
        chan_->onReconnect(
            [this, sockchan]() {
                sockman_->add(sockchan);
            });
        sockman_->add(sockchan);
        sockman_->run();
    }
}

pair<shared_ptr<Filesystem>, string>
NfsFilesystemFactory::mount(FilesystemManager* fsman, const string& url)
{
    using namespace oncrpc;

#if 0
    static map<int, string> flavors {
        { AUTH_NONE, "none" },
        { AUTH_SYS, "sys" },
        { RPCSEC_GSS_KRB5, "krb5" },
        { RPCSEC_GSS_KRB5I, "krb5i" },
        { RPCSEC_GSS_KRB5P, "krb5p" },
    };
#endif

    UrlParser p(url);

    auto chan = Channel::open(url, "tcp");
    auto client = make_shared<oncrpc::SysClient>(NFS4_PROGRAM, NFS_V4);
    auto clock = make_shared<detail::SystemClock>();

    string clientowner;
    if (FLAGS_clientowner.size() > 0) {
        clientowner = FLAGS_clientowner;
    }
    else {
        char hostname[256];
        if (::gethostname(hostname, sizeof(hostname)) < 0)
            throw system_error(errno, system_category());
        ostringstream ss;
        ss << "unfscl" << ::getpgrp() << "@" << hostname;
        clientowner = ss.str();
    }

    return make_pair(
        fsman->mount<NfsFilesystem>(
            p.host + ":/", chan, client, clock, clientowner),
        p.path);
}
