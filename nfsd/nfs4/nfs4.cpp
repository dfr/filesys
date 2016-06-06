/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <rpc++/server.h>
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

static shared_ptr<NfsServer> nfsService;
static shared_ptr<DataServer> dataService;

void nfsd::nfs4::init(
    shared_ptr<ServiceRegistry> svcreg,
    shared_ptr<ThreadPool> threadpool,
    const vector<int>& sec,
    const vector<AddressInfo>& addrs)
{
    using placeholders::_1;

    nfsService = make_shared<NfsServer>(sec);
    threadpool->addService(
        NFS4_PROGRAM, NFS_V4, svcreg,
        std::bind(&NfsServer::dispatch, nfsService.get(), _1));

    dataService = make_shared<DataServer>(sec);
    threadpool->addService(
        DISTFS_DS, DS_V1, svcreg,
        std::bind(&DataServer::dispatch, dataService.get(), _1));
}
