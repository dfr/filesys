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

// -*- c++ -*-
#pragma once

#include <filesys/filesys.h>
#include "filesys/nfs4/nfs4proto.h"

namespace nfsd {
namespace nfs4 {

class NfsClient;
class NfsState;

/// Track per-file state such as open files, locks, delegations etc.
class NfsFileState
{
public:
    NfsFileState(std::shared_ptr<filesys::File> file);

    auto& fh() const { return fh_; }

    auto lock()
    {
        return std::unique_lock<std::mutex>(mutex_);
    }

    void addOpen(std::shared_ptr<NfsState> ns);

    void removeOpen(std::shared_ptr<NfsState> ns);

    void addDelegation(std::shared_ptr<NfsState> ns);

    void removeDelegation(std::shared_ptr<NfsState> ns);

    void addLayout(std::shared_ptr<NfsState> ns);

    void removeLayout(std::shared_ptr<NfsState> ns);

    /// Revoke a state object associated with this file. This does not
    /// increment the client's revoked state count since it may be
    /// called with the client lock held. The caller must take
    /// responsibility for maintaining the revoked state count
    void revoke(std::shared_ptr<NfsState> ns)
    {
        auto lk = lock();
        revoke(lk, ns);
    }

    std::shared_ptr<NfsState> findOpen(
        std::shared_ptr<NfsClient> client,
        const filesys::nfs4::open_owner4& owner)
    {
        auto lk = lock();
        return findOpen(lk, client, owner);
    }

    std::shared_ptr<NfsState> findOpen(
        std::unique_lock<std::mutex>& lock,
        std::shared_ptr<NfsClient> client,
        const filesys::nfs4::open_owner4& owner);

    bool isOpen(
        std::shared_ptr<NfsClient> client)
    {
        auto lk = lock();
        return isOpen(lk, client);
    }

    bool isOpen(
        std::unique_lock<std::mutex>& lock,
        std::shared_ptr<NfsClient> client);

    std::shared_ptr<NfsState> findDelegation(
        std::shared_ptr<NfsClient> client)
    {
        auto lk = lock();
        return findDelegation(lk, client);
    }

    std::shared_ptr<NfsState> findDelegation(
        std::unique_lock<std::mutex>& lock,
        std::shared_ptr<NfsClient> client);

    std::shared_ptr<NfsState> findLayout(
        std::shared_ptr<NfsClient> client)
    {
        auto lk = lock();
        return findLayout(lk, client);
    }

    std::shared_ptr<NfsState> findLayout(
        std::unique_lock<std::mutex>& lock,
        std::shared_ptr<NfsClient> client);

    /// Check if this owner can open with the given access and deny
    /// share reservation. If the open is not possible, throw an
    /// appropriate nfsstat4 exception (either NFS4ERR_SHARE_DENIED or
    /// NFS4ERR_GRACE depending on the restored flag of the
    /// conflicting state entry).
    void checkShare(
        std::unique_lock<std::mutex>& lock,
        std::shared_ptr<NfsClient> client,
        const filesys::nfs4::open_owner4& owner,
        int access, int deny, bool inGracePeriod);

    void updateShare(std::shared_ptr<NfsState> ns)
    {
        auto lk = lock();
        updateShare(lk, ns);
    }

    void updateShare(
        std::unique_lock<std::mutex>& lock,
        std::shared_ptr<NfsState> ns);

    void updateShare(std::unique_lock<std::mutex>& lock);

    bool hasState() const
    {
        return (opens_.size() + delegations_.size() + layouts_.size()) > 0;
    }

    auto access() const { return access_; }
    auto deny() const { return deny_; }
    auto& opens() const { return opens_; }
    auto& delegations() const { return delegations_; }
    auto& layouts() const { return layouts_; }

private:
    void revoke(
        std::unique_lock<std::mutex>& lock, std::shared_ptr<NfsState> ns);

    std::mutex mutex_;
    filesys::nfs4::nfs_fh4 fh_;
    int access_ = 0;
    int deny_ = 0;
    std::unordered_set<std::shared_ptr<NfsState>> opens_;
    std::unordered_set<std::shared_ptr<NfsState>> delegations_;
    std::unordered_set<std::shared_ptr<NfsState>> layouts_;
};

}
}
