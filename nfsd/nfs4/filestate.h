/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include <fs++/filesys.h>
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

    /// Return true if this owner can open with the given access and
    /// deny share reservation
    bool checkShare(
        std::shared_ptr<NfsClient> client,
        const filesys::nfs4::open_owner4& owner,
        int access, int deny);

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
