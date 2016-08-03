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

MountServer::MountServer(const vector<int>& sec, shared_ptr<Filesystem> fs)
    : sec_(sec),
      fs_(fs)
{
}

void MountServer::null()
{
}

mountres3 MountServer::mnt(const dirpath& dir)
{
    VLOG(1) << "MountServer::mnt(" << dir << ")";

    // RFC1816 5.2.1: AUTH_UNIX authentication or better is required
    auto& ctx = CallContext::current();
    if (ctx.flavor() < AUTH_SYS) {
        ctx.authError(AUTH_TOOWEAK);
        throw NoReply();
    }

    // We only have one filesystem exported as "/"
    if (dir == "/") {
        mountres3_ok res;
        FileHandle fh = fs_->root()->handle();
        oncrpc::XdrMemory xm(FHSIZE3);
        xdr(fh, static_cast<oncrpc::XdrSink*>(&xm));
        res.fhandle.resize(xm.writePos());
        copy_n(xm.buf(), xm.writePos(), res.fhandle.data());
        res.auth_flavors = sec_;
        return mountres3(MNT3_OK, move(res));
    }
    return mountres3(MNT3ERR_NOENT);
}

mountlist MountServer::dump()
{
    VLOG(1) << "MountServer::dump()";
    return nullptr;
}

void MountServer::umnt(const dirpath& dir)
{
    VLOG(1) << "MountServer::umnt(" << dir << ")";
    // RFC1816 5.2.3: AUTH_UNIX authentication or better is required
    auto& ctx = CallContext::current();
    if (ctx.flavor() < AUTH_SYS) {
        ctx.authError(AUTH_TOOWEAK);
        throw NoReply();
    }
}

void MountServer::umntall()
{
    VLOG(1) << "MountServer::umntall()";
    // RFC1816 5.2.4: AUTH_UNIX authentication or better is required
    auto& ctx = CallContext::current();
    if (ctx.flavor() < AUTH_SYS) {
        ctx.authError(AUTH_TOOWEAK);
        throw NoReply();
    }
}

exports MountServer::listexports()
{
    VLOG(1) << "MountServer::listexports()";
    unique_ptr<exportnode> res = make_unique<exportnode>();
    res->ex_dir = dirpath("/");
    return res;
}
