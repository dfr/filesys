#include "nfs4proto.h"
#include "nfs4util.h"

using namespace filesys::nfs4;
using namespace std;

system_error filesys::nfs4::mapStatus(nfsstat4 stat)
{
    static unordered_map<int, int> statusMap = {
        { NFS4ERR_PERM, EPERM },
        { NFS4ERR_NOENT, ENOENT },
        { NFS4ERR_IO, EIO },
        { NFS4ERR_NXIO, ENXIO },
        { NFS4ERR_ACCESS, EACCES },
        { NFS4ERR_EXIST, EEXIST },
        { NFS4ERR_XDEV, EXDEV },
        { NFS4ERR_NOTDIR, ENOTDIR },
        { NFS4ERR_ISDIR, EISDIR },
        { NFS4ERR_INVAL, EINVAL },
        { NFS4ERR_FBIG, EFBIG },
        { NFS4ERR_NOSPC, ENOSPC },
        { NFS4ERR_ROFS, EROFS },
        { NFS4ERR_MLINK, EMLINK },
        { NFS4ERR_NAMETOOLONG, ENAMETOOLONG },
        { NFS4ERR_NOTEMPTY, ENOTEMPTY },
        { NFS4ERR_DQUOT, EDQUOT },
        { NFS4ERR_STALE, ESTALE },
        { NFS4ERR_NOTSUPP, EOPNOTSUPP },
    };
    auto i = statusMap.find(int(stat));
    if (i != statusMap.end())
           return system_error(i->second, system_category());
    else
           return system_error(EINVAL, system_category());
}
