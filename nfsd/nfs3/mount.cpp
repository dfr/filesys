#include <fs++/filesys.h>

#include "mount.h"

using namespace filesys;
using namespace filesys::nfs;
using namespace nfsd;
using namespace nfsd::nfs3;
using namespace oncrpc;
using namespace std;

MountServer::MountServer()
{
}

void MountServer::null()
{
}

mountres3 MountServer::mnt(const dirpath& dir)
{
    for (auto& entry: FilesystemManager::instance()) {
        if (dir == entry.first) {
            mountres3_ok res;
            FileHandle fh;
            entry.second->root()->handle(fh);
            oncrpc::XdrMemory xm(FHSIZE3);
            xdr(fh, static_cast<oncrpc::XdrSink*>(&xm));
            res.fhandle.resize(xm.writePos());
            copy_n(xm.buf(), xm.writePos(), res.fhandle.data());
            res.auth_flavors.push_back(oncrpc::AUTH_SYS);
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
}

void MountServer::umntall()
{
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
