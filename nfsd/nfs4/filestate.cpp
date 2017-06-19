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

#include <glog/logging.h>

#include "client.h"
#include "filestate.h"
#include "util.h"

using namespace filesys;
using namespace filesys::nfs4;
using namespace nfsd::nfs4;
using namespace std;

NfsFileState::NfsFileState(shared_ptr<File> file)
    : fh_(exportFileHandle(file))
{
}

void NfsFileState::addOpen(shared_ptr<NfsState> ns)
{
    unique_lock<mutex> lock(mutex_);
    opens_.insert(ns);
    updateShare(lock, ns);
}

void NfsFileState::removeOpen(shared_ptr<NfsState> ns)
{
    unique_lock<mutex> lock(mutex_);
    opens_.erase(ns);
    updateShare(lock);
}

void NfsFileState::addDelegation(shared_ptr<NfsState> ns)
{
    unique_lock<mutex> lock(mutex_);
    delegations_.insert(ns);
}

void NfsFileState::removeDelegation(shared_ptr<NfsState> ns)
{
    unique_lock<mutex> lock(mutex_);
    delegations_.erase(ns);
}

void NfsFileState::addLayout(shared_ptr<NfsState> ns)
{
    unique_lock<mutex> lock(mutex_);
    layouts_.insert(ns);
}

void NfsFileState::removeLayout(shared_ptr<NfsState> ns)
{
    unique_lock<mutex> lock(mutex_);
    layouts_.erase(ns);
}

shared_ptr<NfsState> NfsFileState::findOpen(
    unique_lock<mutex>& lock, shared_ptr<NfsClient> client,
    const open_owner4& owner)
{
    for (auto ns: opens_)
        if (ns->client() == client && ns->owner() == owner)
            return ns;
    return nullptr;
}

bool NfsFileState::isOpen(
    unique_lock<mutex>& lock, shared_ptr<NfsClient> client)
{
    for (auto ns: opens_)
        if (ns->client() == client)
            return true;
    return false;
}

shared_ptr<NfsState> NfsFileState::findDelegation(
    unique_lock<mutex>& lock, shared_ptr<NfsClient> client)
{
    for (auto ns: delegations_)
        if (ns->client() == client)
            return ns;
    return nullptr;
}

shared_ptr<NfsState> NfsFileState::findLayout(
    unique_lock<std::mutex>& lock, shared_ptr<NfsClient> client)
{
    for (auto ns: layouts_)
        if (ns->client() == client)
            return ns;
    return nullptr;
}

/// Return true if this owner can open with the given access and
/// deny share reservation
void NfsFileState::checkShare(
    unique_lock<mutex>& lock,
    shared_ptr<NfsClient> client,
    const filesys::nfs4::open_owner4& owner,
    int access, int deny, bool inGracePeriod)
{
    // If the requested reservation conflicts with the current state
    // and that conflicting reservation is not owned by this owner,
    // deny the reservation
retry:
    if ((access & deny_) || (deny & access_)) {
        auto conflictingAccess = access & deny_;
        auto conflictingDeny = deny & access_;
        auto ns = findOpen(lock, client, owner);
        if (ns) {
            // If we have an existing state entry for this owner and
            // it exactly matches the conflict, then there is no
            // actual conflict since this is an upgrade, downgrade or
            // reclaim request
            if ((conflictingAccess & ns->deny()) == conflictingAccess &&
                (conflictingDeny & ns->access()) == conflictingDeny)
                return;
        }
        // Possibly revoke any conflicting state
        for (auto ns: opens_) {
            auto client = ns->client();
            if ((access & ns->deny()) || (deny & ns->access())) {
                // This state entry conflicts
                if (client && client->expired()) {
                    // The owning client has expired - revoke the
                    // conflicting state and retry
                    LOG(INFO) << "Revoking expired stateid: " << ns->id();
                    lock.unlock();
                    ns->client()->revokeState(ns);
                    lock.lock();
                    goto retry;
                }
                else if (ns->restored()) {
                    // The conflicting state was restored from the
                    // database and has not been reclaimed - return
                    // NFS4ERR_GRACE if we are still in the grace
                    // period so the caller can back off and retry.
                    //
                    // Note: the case where a client is reclaiming its
                    // old state will match, is handled above.
                    if (inGracePeriod) {
                        VLOG(1) << "Conflicting unreclaimed stateid: "
                                << ns->id();
                        throw NFS4ERR_GRACE;
                    }
                    else {
                        // The grace period has finished, treat this
                        // state entry as expired
                        LOG(INFO) << "Revoking un-reclaimed stateid: "
                                  << ns->id();
                        lock.unlock();
                        ns->client()->revokeState(ns);
                        lock.lock();
                        goto retry;
                    }
                }
                VLOG(1) << "Conflicting stateid: " << ns->id();
            }
        }
        throw NFS4ERR_SHARE_DENIED;
    }
}

void NfsFileState::updateShare(
    unique_lock<mutex>& lock, shared_ptr<NfsState> ns)
{
    access_ |= ns->access();
    deny_ |= ns->deny();
}

void NfsFileState::updateShare(unique_lock<mutex>& lock)
{
    int a = 0, d = 0;
    for (auto ns: opens_) {
        a |= ns->access();
        d |= ns->deny();

        // We can stop if the newly calculated values are equal to
        // the current values. Note that more restrictive values
        // for access and deny are handled above in
        // updateShare(shared_ptr<NfsState>)
        if (a == access_ && d == deny_)
            return;
    }
    access_ = a;
    deny_ = d;
}

void NfsFileState::revoke(
    std::unique_lock<std::mutex>& lock, std::shared_ptr<NfsState> ns)
{
    ns->revoke();
    switch (ns->type()) {
    case StateType::OPEN:
        opens_.erase(ns);
        break;
    case StateType::DELEGATION:
        delegations_.erase(ns);
        break;
    case StateType::LAYOUT:
        layouts_.erase(ns);
        break;
    }
    updateShare(lock);
}
