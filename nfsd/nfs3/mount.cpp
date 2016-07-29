/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <filesys/filesys.h>
#include <rpc++/cred.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "mount.h"

using namespace filesys;
using namespace filesys::nfs3;
using namespace nfsd;
using namespace nfsd::nfs3;
using namespace oncrpc;
using namespace std;

MountServer::MountServer(const vector<int>& sec)
    : sec_(sec)
{
}

void MountServer::null()
{
}

mountres3 MountServer::mnt(const dirpath& dir)
{
    LOG(INFO) << "MountServer::mnt(" << dir << ")";

    // RFC1816 5.2.1: AUTH_UNIX authentication or better is required
    auto& ctx = CallContext::current();
    if (ctx.flavor() < AUTH_SYS) {
        ctx.authError(AUTH_TOOWEAK);
        throw NoReply();
    }

    for (auto& entry: FilesystemManager::instance()) {
        VLOG(1) << "Checking mount point " << entry.first;
        if (dir == entry.first || dir == "/" + entry.first) {
            mountres3_ok res;
            FileHandle fh = entry.second->root()->handle();
            oncrpc::XdrMemory xm(FHSIZE3);
            xdr(fh, static_cast<oncrpc::XdrSink*>(&xm));
            res.fhandle.resize(xm.writePos());
            copy_n(xm.buf(), xm.writePos(), res.fhandle.data());
            res.auth_flavors = sec_;
            return mountres3(MNT3_OK, move(res));
        }
    }
    return mountres3(MNT3ERR_NOENT);
}

mountlist MountServer::dump()
{
    return nullptr;
}

void MountServer::umnt(const dirpath& dir)
{
    // RFC1816 5.2.3: AUTH_UNIX authentication or better is required
    auto& ctx = CallContext::current();
    if (ctx.flavor() < AUTH_SYS) {
        ctx.authError(AUTH_TOOWEAK);
        throw NoReply();
    }
}

void MountServer::umntall()
{
    // RFC1816 5.2.4: AUTH_UNIX authentication or better is required
    auto& ctx = CallContext::current();
    if (ctx.flavor() < AUTH_SYS) {
        ctx.authError(AUTH_TOOWEAK);
        throw NoReply();
    }
}

exports MountServer::listexports()
{
    unique_ptr<exportnode> res;
    unique_ptr<exportnode>* p = &res;
    for (auto& entry: FilesystemManager::instance()) {
        *p = make_unique<exportnode>();
        (*p)->ex_dir = dirpath(entry.first);
        p = &(*p)->ex_next;
    }
    return res;
}
