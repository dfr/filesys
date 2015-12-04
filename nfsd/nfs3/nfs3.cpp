#include <rpc++/server.h>
#include <rpc++/pmap.h>
#include <rpc++/rpcbind.h>
#include <glog/logging.h>

#include "nfs3.h"
#include "mount.h"
#include "nfs3server.h"

using namespace filesys::nfs;
using namespace nfsd;
using namespace nfsd::nfs3;
using namespace oncrpc;
using namespace std;

static shared_ptr<MountServer> mountService;
static shared_ptr<NfsServer> nfsService;

static void bindProgram(
    shared_ptr<Channel> chan, uint32_t prog, uint32_t vers, const AddressInfo& ai)
{
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

void nfsd::nfs3::init(
    shared_ptr<ServiceRegistry> svcreg,
    const vector<int>& sec,
    const vector<AddressInfo>& addrs)
{
    mountService = make_shared<MountServer>(sec);
    nfsService = make_shared<NfsServer>(sec);

    mountService->bind(svcreg);
    nfsService->bind(svcreg);

    if (addrs.size() > 0) {
        LOG(INFO) << "Registering services with rpcbind";
        auto rpcbind = Channel::open("localhost", "sunrpc", "tcp");
        for (auto& ai: addrs) {
            bindProgram(rpcbind, MOUNTPROG, MOUNTVERS3, ai);
            bindProgram(rpcbind, NFS_PROGRAM, NFS_V3, ai);
        }
    }
}
