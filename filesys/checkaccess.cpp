/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <system_error>

#include <filesys/filesys.h>

using namespace filesys;

void filesys::CheckAccess(
    int uid, int gid, int mode, const Credential& cred,
    int accmode)
{
    assert((accmode & AccessFlags::ALL) == accmode);

    int granted = 0;
    if (uid == cred.uid()) {
        if (mode & ModeFlags::RUSER)
            granted += AccessFlags::READ;
        if (mode & ModeFlags::WUSER)
            granted += AccessFlags::WRITE;
        if (mode & ModeFlags::XUSER)
            granted += AccessFlags::EXECUTE;
    }
    else if (cred.hasgroup(gid)) {
        if (mode & ModeFlags::RGROUP)
            granted += AccessFlags::READ;
        if (mode & ModeFlags::WGROUP)
            granted += AccessFlags::WRITE;
        if (mode & ModeFlags::XGROUP)
            granted += AccessFlags::EXECUTE;
    }
    else {
        if (mode & ModeFlags::ROTHER)
            granted += AccessFlags::READ;
        if (mode & ModeFlags::WOTHER)
            granted += AccessFlags::WRITE;
        if (mode & ModeFlags::XOTHER)
            granted += AccessFlags::EXECUTE;
    }
    if ((accmode & granted) == accmode)
        return;
    if (!cred.privileged())
        throw std::system_error(EACCES, std::system_category());
}
