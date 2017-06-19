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

#include <rpc++/server.h>
#include <rpc++/pmap.h>
#include <rpc++/rpcbind.h>
#include <glog/logging.h>

#include "nfs3.h"
#include "mount.h"
#include "nfs3server.h"

using namespace filesys::nfs3;
using namespace nfsd;
using namespace nfsd::nfs3;
using namespace oncrpc;
using namespace std;

static shared_ptr<MountServer> mountService;
static shared_ptr<NfsServer> nfsService;

DEFINE_string(rpcbind, "", "URL to use for contacting rpcbind");

static void bindProgram(
    shared_ptr<Channel> chan, uint32_t prog, uint32_t vers, const AddressInfo& ai)
{
    try {
        // portmapper V2
        Portmap pmap(chan);
        mapping mapping;
        mapping.prog = prog;
        mapping.vers = vers;
        mapping.prot = IPPROTO_TCP;
        mapping.port = ai.port();
        pmap.unset(mapping);
        pmap.set(mapping);

        // rpcbind V3
        RpcBind rpcbind(chan);
        rpcb binding;
        binding.r_prog = prog;
        binding.r_vers = vers;
        binding.r_netid = "";
        binding.r_addr = "";
        binding.r_owner = ::getenv("USER");
        rpcbind.unset(binding);
        binding.r_netid = ai.netid();
        binding.r_addr = ai.uaddr();
        rpcbind.set(binding);

        LOG(INFO) << "Registered: "
                  << binding.r_prog
                  << "," <<binding.r_vers
                  << "," << binding.r_addr;
    }
    catch (RpcError& e) {
        LOG(ERROR) << "RPC error contacting rpcbind: " << e.what();
    }
}

void nfsd::nfs3::init(
    shared_ptr<ServiceRegistry> svcreg,
    shared_ptr<RestRegistry> restreg,
    shared_ptr<ThreadPool> threadpool,
    const vector<int>& sec,
    const vector<AddressInfo>& addrs,
    shared_ptr<filesys::Filesystem> fs)
{
    mountService = make_shared<MountServer>(sec, fs);
    nfsService = make_shared<NfsServer>(sec, fs);

    mountService->bind(svcreg);
    nfsService->bind(svcreg);
    if (restreg) {
        nfsService->setRestRegistry(restreg);
    }

    if (addrs.size() > 0 && FLAGS_rpcbind.size() > 0) {
        LOG(INFO) << "Registering services with rpcbind";
        auto rpcbind = Channel::open(FLAGS_rpcbind);
        for (auto& ai: addrs) {
            bindProgram(rpcbind, MOUNTPROG, MOUNTVERS3, ai);
            bindProgram(rpcbind, NFS_PROGRAM, NFS_V3, ai);
        }
    }
}

void nfsd::nfs3::shutdown()
{
    mountService.reset();
    nfsService.reset();
}
