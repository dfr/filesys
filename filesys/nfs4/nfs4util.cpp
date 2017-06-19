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
        { NFS4ERR_SHARE_DENIED, EPERM }
    };
    auto i = statusMap.find(int(stat));
    if (i != statusMap.end())
           return system_error(i->second, system_category());
    else
           return system_error(EINVAL, system_category());
}
