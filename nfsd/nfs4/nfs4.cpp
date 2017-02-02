/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
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
