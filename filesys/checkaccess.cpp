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
