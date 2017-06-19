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
#include <rpc++/sockman.h>
#include <glog/logging.h>

#include "nfsd/threadpool.h"
#include "nfs4.h"
#include "server.h"
#include "dataserver.h"

using namespace filesys::nfs4;
using namespace filesys::distfs;
using namespace nfsd;
using namespace nfsd::nfs4;
using namespace oncrpc;
using namespace std;

static shared_ptr<NfsServer> nfs4Service;
static shared_ptr<DataServer> dataService;

static void heartbeat(weak_ptr<oncrpc::SocketManager> sockman)
{
    // Spawn a thread for expireClients to avoid blocking the socket
    // manager thread
    std::thread t(
        []() {
            nfs4Service->expireClients();
        });
    t.detach();
    auto p = sockman.lock();
    if (p) {
        p->add(chrono::system_clock::now() + 1s,
               [=]() {
                   heartbeat(sockman);
               });
    }
}

void nfsd::nfs4::init(
    shared_ptr<oncrpc::SocketManager> sockman,
    shared_ptr<ServiceRegistry> svcreg,
    shared_ptr<RestRegistry> restreg,
    shared_ptr<ThreadPool> threadpool,
    const vector<int>& sec,
    const vector<AddressInfo>& addrs,
    shared_ptr<filesys::Filesystem> fs)
{
    using placeholders::_1;

    nfs4Service = make_shared<NfsServer>(sec, fs, addrs);
    threadpool->addService(
        NFS4_PROGRAM, NFS_V4, svcreg,
        std::bind(&NfsServer::dispatch, nfs4Service.get(), _1));

    if (restreg) {
        nfs4Service->setRestRegistry(restreg);
    }

    dataService = make_shared<DataServer>(sec);
    threadpool->addService(
        DISTFS_DS, DS_V1, svcreg,
        std::bind(&DataServer::dispatch, dataService.get(), _1));

    // Add a timeout to handle client expiry
    weak_ptr<oncrpc::SocketManager> p = sockman;
    sockman->add(chrono::system_clock::now() + 1s,
                 [p]() {
                     heartbeat(p);
                 });
}

void nfsd::nfs4::shutdown()
{
    nfs4Service.reset();
    dataService.reset();
}
