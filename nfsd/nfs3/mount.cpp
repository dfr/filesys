/*-
 * Copyright (c) 2016-present Doug Rabson
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
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
