/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

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
        keyval::Database* db,
        filesys::nfs4::clientid4 id,
        const filesys::nfs4::client_owner4& owner,
        const std::string& principal,
        filesys::detail::Clock::time_point expiry,
        const filesys::nfs4::state_protect4_a& spa);
    NfsClient(
        keyval::Database* db,
        filesys::nfs4::clientid4 id,
        std::unique_ptr<keyval::Iterator>& iterator,
        filesys::detail::Clock::time_point expiry);

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
    void setExpiry(filesys::detail::Clock::time_point expiry)
    {
        expired_ = false;
        expiry_ = expiry;
    }

    void releaseState()
    {
        state_.clear();
        revokedState_.clear();
        recallableStateCount_ = 0;
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
        filesys::detail::Clock::time_point expiry)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto id = newStateId();
        auto ns = std::make_shared<NfsState>(
            stateNS_, StateType::OPEN, id, shared_from_this(), fs,
            owner, access, deny, of, expiry);
        state_[id] = ns;
        fs->addOpen(ns);
        return ns;
    }

    std::shared_ptr<NfsState> addDelegation(
        std::shared_ptr<NfsFileState> fs,
        int access,
        std::shared_ptr<filesys::OpenFile> of,
        filesys::detail::Clock::time_point expiry)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto id = newStateId();
        auto ns = std::make_shared<NfsState>(
            stateNS_, StateType::DELEGATION, id, shared_from_this(), fs,
            filesys::nfs4::state_owner4{id_, {}}, access, 0, of, expiry);
        assert(state_.find(id) == state_.end());
        recallableStateCount_++;
        state_[id] = ns;
        fs->addDelegation(ns);
        return ns;
    }

    std::shared_ptr<NfsState> addLayout(
        std::shared_ptr<NfsFileState> fs,
        filesys::nfs4::layoutiomode4 iomode,
        const std::vector<std::shared_ptr<filesys::Device>>& devices,
        filesys::detail::Clock::time_point expiry)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto id = newStateId();
        int access = (iomode == filesys::nfs4::LAYOUTIOMODE4_RW ?
                      filesys::nfs4::OPEN4_SHARE_ACCESS_BOTH :
                      filesys::nfs4::OPEN4_SHARE_ACCESS_READ);
        auto ns = std::make_shared<NfsState>(
            stateNS_, StateType::LAYOUT, id, shared_from_this(), fs,
            filesys::nfs4::state_owner4{id_, {}}, access, 0, nullptr, expiry);
        for (auto dev: devices) {
            auto& ds = devices_[dev];
            ds.layouts.insert(ns);
            checkDeviceState(lock, dev, ds);
        }
        ns->setDevices(devices);
        assert(state_.find(id) == state_.end());
        recallableStateCount_++;
        state_[id] = ns;
        fs->addLayout(ns);
        return ns;
    }

    void clearState(const filesys::nfs4::stateid4& stateid);
    void clearLayouts();
    void revokeState(std::shared_ptr<NfsState> ns);
    void revokeState(std::shared_ptr<NfsState> ns, keyval::Transaction* trans);
    void revokeState(keyval::Transaction* trans);
    void revokedUnreclaimedState(keyval::Transaction* trans);

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

    void expireState(filesys::detail::Clock::time_point now);

private:
    std::mutex mutex_;
    keyval::Database* db_;
    std::shared_ptr<keyval::Namespace> clientsNS_;
    std::shared_ptr<keyval::Namespace> stateNS_;
    filesys::nfs4::clientid4 id_;
    ClientData data_;
    filesys::nfs4::nfs_impl_id4 impl_;
    bool expired_ = false;
    filesys::detail::Clock::time_point expiry_;
    bool confirmed_ = false;
    bool restored_ = false;
    std::atomic_int nextSessionIndex_;
    std::unordered_set<std::shared_ptr<NfsSession>> sessions_;
    std::shared_ptr<oncrpc::RestRegistry> restreg_;

    // Device state tracking
    std::unordered_map<
        std::shared_ptr<filesys::Device>,
        DeviceState> devices_;

    // Current state
    std::atomic_int nextStateIndex_;
    std::unordered_map<
        filesys::nfs4::stateid4,
        std::shared_ptr<NfsState>,
        filesys::nfs4::NfsStateidHashIgnoreSeqid,
        filesys::nfs4::NfsStateidEqualIgnoreSeqid> state_;
    std::unordered_map<
        filesys::nfs4::stateid4,
        std::shared_ptr<NfsState>,
        filesys::nfs4::NfsStateidHashIgnoreSeqid,
        filesys::nfs4::NfsStateidEqualIgnoreSeqid> revokedState_;
    int recallableStateCount_ = 0;

    // Pseudo-slot for create_session
    filesys::nfs4::sequenceid4 sequence_;
    filesys::nfs4::CREATE_SESSION4res reply_;
};

}
}
