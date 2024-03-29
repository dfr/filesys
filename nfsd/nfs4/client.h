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

#include <list>
#include <unordered_set>
#include <filesys/filesys.h>
#include <rpc++/rest.h>
#include "filesys/nfs4/nfs4proto.h"
#include "filesys/nfs4/nfs4util.h"

#include "filestate.h"
#include "state.h"

DECLARE_int32(max_state);

namespace keyval {
class Database;
class Iterator;
class Namespace;
class Transaction;
}

namespace nfsd {
namespace nfs4 {

struct CompoundState;
class NfsFileState;
class NfsServer;
class NfsSession;
class NfsState;

// Client objects are stored in the database using the
// owner.co_ownerid as key and ClientData as value
struct ClientData
{
    filesys::nfs4::client_owner4 owner;
    std::string principal;
    filesys::nfs4::state_protect_how4 spa;
};

template <class XDR>
void xdr(oncrpc::RefType<ClientData, XDR> v, XDR* xdrs)
{
    xdr(v.owner, xdrs);
    xdr(v.principal, xdrs);
    xdr(v.spa, xdrs);
}

class NfsClient: public oncrpc::RestHandler,
                 public std::enable_shared_from_this<NfsClient>
{
    struct DeviceState {
        filesys::Device::CallbackHandle callback = 0;
        int notifications = 0;
        std::unordered_set<std::shared_ptr<NfsState>> layouts;
    };

public:
    NfsClient(
        std::shared_ptr<keyval::Database> db,
        filesys::nfs4::clientid4 id,
        const filesys::nfs4::client_owner4& owner,
        const std::string& principal,
        util::Clock::time_point expiry,
        const filesys::nfs4::state_protect4_a& spa);
    NfsClient(
        std::shared_ptr<keyval::Database> db,
        filesys::nfs4::clientid4 id,
        std::unique_ptr<keyval::Iterator>& iterator,
        util::Clock::time_point expiry);

    ~NfsClient();

    // RestHandler overrides
    bool get(
        std::shared_ptr<oncrpc::RestRequest> req,
        std::unique_ptr<oncrpc::RestEncoder>&& res) override;
    bool post(
        std::shared_ptr<oncrpc::RestRequest> req,
        std::unique_ptr<oncrpc::RestEncoder>&& res) override;

    void encodeState(
        std::unique_ptr<oncrpc::RestEncoder>&& enc,
        std::shared_ptr<NfsState> ns);

    void setRestRegistry(std::shared_ptr<oncrpc::RestRegistry> restreg);

    auto id() const { return id_; }
    auto& owner() const { return data_.owner; }
    auto& principal() const { return data_.principal; }
    auto confirmed() const { return confirmed_; }
    auto restored() const { return restored_; }
    auto sequence() const { return sequence_; }
    auto& reply() const { return reply_; }
    auto spa() const { return data_.spa; }
    auto& state() const { return state_; }
    auto& sessions() const { return sessions_; }

    void save(keyval::Transaction* trans);
    void remove(keyval::Transaction* trans);

    int stateCount()
    {
        return int(state_.size());
    }

    bool hasState()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return state_.size() > 0;
    }

    bool hasRevokedState()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return revokedState_.size() > 0;
    }

    auto expired() const { return expired_; }
    void setExpired()
    {
        expired_ = true;
    }

    auto expiry() const { return expiry_; }
    void setExpiry(util::Clock::time_point expiry)
    {
        expired_ = false;
        expiry_ = expiry;
    }

    void setConfirmed()
    {
        confirmed_ = true;
    }

    void setRestored()
    {
        restored_ = true;
    }

    void clearRestored()
    {
        restored_ = false;
    }

    void setReply(const filesys::nfs4::CREATE_SESSION4res& reply);

    filesys::nfs4::sessionid4 newSessionId();

    void addSession(std::shared_ptr<NfsSession> session)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        sessions_.insert(session);
    }

    void removeSession(std::shared_ptr<NfsSession> session)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        sessions_.erase(session);
    }

    int sessionCount()
    {
        return sessions_.size();
    }

    filesys::nfs4::stateid4 newStateId();

    std::shared_ptr<NfsState> findState(
        const CompoundState& state,
        const filesys::nfs4::stateid4& stateid,
        bool allowRevoked = false);

    std::shared_ptr<NfsState> addOpen(
        std::shared_ptr<NfsFileState> fs,
        const filesys::nfs4::state_owner4& owner,
        int access,
        int deny,
        std::shared_ptr<filesys::OpenFile> of,
        util::Clock::time_point expiry);
    std::shared_ptr<NfsState> addDelegation(
        std::shared_ptr<NfsFileState> fs,
        int access,
        std::shared_ptr<filesys::OpenFile> of,
        util::Clock::time_point expiry);
    std::shared_ptr<NfsState> addLayout(
        std::shared_ptr<NfsFileState> fs,
        filesys::nfs4::layoutiomode4 iomode,
        const std::vector<std::shared_ptr<filesys::Device>>& devices,
        util::Clock::time_point expiry);

    void clearState();
    void clearState(const filesys::nfs4::stateid4& stateid);
    void clearLayouts();
    void revokeState(std::shared_ptr<NfsState> ns);
    void revokeState(std::shared_ptr<NfsState> ns, keyval::Transaction* trans);
    void revokeState();
    void revokeUnreclaimedState();
    void revokeAndClear(std::shared_ptr<NfsState> ns);

    void deviceCallback(
        std::shared_ptr<filesys::Device> dev,
        filesys::Device::State state);

    void setDeviceNotify(std::shared_ptr<filesys::Device> dev, int mask)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto& ds = devices_[dev];
        ds.notifications = mask;
        checkDeviceState(lock, dev, ds);
    }

    void clearLayout(
        std::unique_lock<std::mutex>& lock,
        std::shared_ptr<NfsState> ns)
    {
        for (auto dev: ns->devices()) {
            auto& ds = devices_[dev];
            ds.layouts.erase(ns);
            checkDeviceState(lock, dev, ds);
        }
    }

    void checkDeviceState(
        std::unique_lock<std::mutex>& lock,
        std::shared_ptr<filesys::Device> dev,
        DeviceState& ds)
    {
        if (ds.notifications == 0 && ds.layouts.size() == 0) {
            if (ds.callback)
                dev->removeStateCallback(ds.callback);
            devices_.erase(dev);
        }
        else if (!ds.callback) {
            ds.callback = dev->addStateCallback(
                [dev, this](filesys::Device::State state) {
                    deviceCallback(dev, state);
                });
        }
    }

    void sendRecallAny();

    void updateExpiry(
        std::shared_ptr<NfsState>,
        util::Clock::time_point oldExpiry,
        util::Clock::time_point newExpiry);

    void expireState(util::Clock::time_point now);

    void reportRevoked();

private:
    std::mutex mutex_;
    std::shared_ptr<keyval::Database> db_;
    std::shared_ptr<keyval::Namespace> clientsNS_;
    std::shared_ptr<keyval::Namespace> stateNS_;
    filesys::nfs4::clientid4 id_;
    ClientData data_;
    filesys::nfs4::nfs_impl_id4 impl_;
    bool expired_ = false;
    util::Clock::time_point expiry_;
    bool confirmed_ = false;
    bool restored_ = false;
    std::atomic_int nextSessionIndex_;
    std::unordered_set<std::shared_ptr<NfsSession>> sessions_;
    std::weak_ptr<oncrpc::RestRegistry> restreg_;

    // Device state tracking
    std::unordered_map<
        std::shared_ptr<filesys::Device>,
        DeviceState> devices_;

    // Current state
    typedef std::list<std::shared_ptr<NfsState>> statesT;
    std::atomic_int nextStateIndex_;
    std::unordered_map<
        filesys::nfs4::stateid4,
        statesT::iterator,
        filesys::nfs4::NfsStateidHashIgnoreSeqid,
        filesys::nfs4::NfsStateidEqualIgnoreSeqid> state_;
    std::unordered_map<
        filesys::nfs4::stateid4,
        std::shared_ptr<NfsState>,
        filesys::nfs4::NfsStateidHashIgnoreSeqid,
        filesys::nfs4::NfsStateidEqualIgnoreSeqid> revokedState_;
    statesT orderedState_;         // roughly ordered by expiry
    int recallableStateCount_ = 0;

    // Pseudo-slot for create_session
    filesys::nfs4::sequenceid4 sequence_;
    filesys::nfs4::CREATE_SESSION4res reply_;
};

}
}
