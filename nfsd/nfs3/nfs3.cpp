#include <rpc++/server.h>

#include "nfs3.h"
#include "mount.h"
#include "nfs3server.h"

using namespace nfsd;
using namespace nfsd::nfs3;
using namespace oncrpc;
using namespace std;

static shared_ptr<MountServer> mountService;
static shared_ptr<NfsServer> nfsService;

void nfsd::nfs3::init(shared_ptr<ServiceRegistry> svcreg)
{
    mountService = make_shared<MountServer>();
    nfsService = make_shared<NfsServer>();

    mountService->bind(svcreg);
    nfsService->bind(svcreg);
}
