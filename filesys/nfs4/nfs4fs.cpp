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

#include <rpc++/urlparser.h>

#include "nfs4fs.h"

using namespace filesys;
using namespace filesys::nfs4;
using namespace std;

DEFINE_string(clientowner, "", "Override default client owner");

static random_device rnd;

NfsFilesystem::NfsFilesystem(
    shared_ptr<oncrpc::Channel> chan,
    shared_ptr<oncrpc::Client> client,
    shared_ptr<util::Clock> clock,
    const string& clientowner,
    shared_ptr<IIdMapper> idmapper)
    : clock_(clock),
      idmapper_(idmapper),
      sockman_(make_shared<oncrpc::SocketManager>()),
      svcreg_(chan->serviceRegistry()),
      cbsvc_(this)
{
    // Only allow a fraction of the file cache to have active delegations
    delegations_.setSizeLimit(cache_.sizeLimit() / 8);

    // We need a service registry for our callback program
    if (!svcreg_) {
        svcreg_ = make_shared<oncrpc::ServiceRegistry>();
        chan->setServiceRegistry(svcreg_);
    }

    // Allocate a program number from the transient range
    cbprog_ = svcreg_->allocate(0x40000000, 0x5fffffff);
    cbsvc_.bind(cbprog_, svcreg_);

    // Initialise the wire protocol, creating client and session
    proto_ = make_shared<NfsProto>(this, chan, client, clientowner, cbprog_);

    // Handle callbacks on a new thread
    cbthread_ = thread([this, chan](){ handleCallbacks(); });
}

NfsFilesystem::NfsFilesystem(
    shared_ptr<oncrpc::Channel> chan,
    shared_ptr<oncrpc::Client> client,
    shared_ptr<util::Clock> clock,
    const string& clientowner)
    : NfsFilesystem(chan, client, clock, clientowner, LocalIdMapper())
{
}

NfsFilesystem::~NfsFilesystem()
{
    if (proto_) {
        unmount();
        proto_.reset();
    }

    sockman_->stop();
    cbsvc_.unbind(cbprog_, svcreg_);
    cbthread_.join();
}

shared_ptr<File>
NfsFilesystem::root()
{
    if (!root_) {
        nfs_fh4 rootfh;
        fattr4 rootattr;
        proto_->compound(
            "root",
            [](auto& enc)
            {
                bitmap4 wanted;
                setSupportedAttrs(wanted);
                set(wanted, FATTR4_MAXREAD);
                set(wanted, FATTR4_MAXWRITE);
                set(wanted, FATTR4_LEASE_TIME);
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

        proto_->fsinfo().maxRead = root_->attr().maxread_;
        proto_->fsinfo().maxWrite = root_->attr().maxwrite_;
        proto_->fsinfo().leaseTime = root_->attr().lease_time_;

        // Set the buffer size for the largest read or write request we will
        // make, allowing extra space for protocol overhead
        proto_->channel()->setBufferSize(
            512 + max(proto_->fsinfo().maxRead, proto_->fsinfo().maxWrite));
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
    VLOG(1) << "Returning delegations";
    delegations_.clear();

    VLOG(1) << "Closing open files";
    auto lock = cache_.lock();
    for (auto& e: cache_) {
        e.second->close();
    }

    proto_.reset();
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
        VLOG(1) << "Adding read delegation " << delegation.read().stateid;
        stateid = delegation.read().stateid;
        break;

    case OPEN_DELEGATE_WRITE:
        VLOG(1) << "Adding write delegation " << delegation.write().stateid;
        stateid = delegation.write().stateid;
        break;
    }
    auto d = make_shared<NfsDelegation>(proto_, file, of, move(delegation));
    delegations_.add(stateid, d);
    return d;
}

void
NfsFilesystem::freeRevokedState()
{
    auto lock = cache_.lock(try_to_lock);
    if (lock) {
        LOG(INFO) << "Freeing revoked state";
        for (auto& e: cache_) {
            e.second->testState();
        }
    }
}

void
NfsFilesystem::recover()
{
    LOG(INFO) << "Recovering state";
    auto lock = cache_.lock();
    for (auto& e: cache_) {
        e.second->recover();
    }
    proto_->compound(
        "reclaim_complete",
        [](auto& enc)
        {
            enc.reclaim_complete(false);
        },
        [](auto& dec)
        {
            dec.reclaim_complete();
        });
}

void
NfsFilesystem::handleCallbacks()
{
    if (proto_) {
        auto chan = proto_->channel();
        auto sockchan = dynamic_pointer_cast<oncrpc::Socket>(chan);
        if (sockchan) {
            chan->onReconnect(
                [this, sockchan]() {
                    sockman_->add(sockchan);
                });
            sockman_->add(sockchan);
            sockman_->run();
        }
    }
}

shared_ptr<Filesystem>
NfsFilesystemFactory::mount(
    const string& url, shared_ptr<oncrpc::SocketManager> sockman)
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
    auto clock = make_shared<util::SystemClock>();

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

    return make_shared<NfsFilesystem>(chan, client, clock, clientowner);
}
