#include <rpc++/server.h>
#include <rpc++/pmap.h>
#include <rpc++/rpcbind.h>
#include <glog/logging.h>

#include "nfsd/threadpool.h"
#include "nfsd/nfs4/nfs4.h"
#include "nfsd/nfs4/nfs4server.h"

using namespace filesys::nfs4;
using namespace nfsd;
using namespace nfsd::nfs4;
using namespace oncrpc;
using namespace std;

static shared_ptr<NfsServer> nfsService;

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
}
