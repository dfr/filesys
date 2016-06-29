/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include <unordered_set>
#include <fs++/filesys.h>
#include "filesys/nfs4/nfs4proto.h"
#include "filesys/nfs4/nfs4util.h"

#include "filestate.h"
#include "state.h"

DECLARE_int32(max_state);

namespace nfsd {
namespace nfs4 {

struct CompoundState;
class NfsFileState;
class NfsSession;
class NfsState;

class NfsClient: public std::enable_shared_from_this<NfsClient>
{
    struct DeviceState {
        filesys::Device::CallbackHandle callback = 0;
        int notifications = 0;
        std::unordered_set<std::shared_ptr<NfsState>> layouts;
    };

public:
    NfsClient(
        filesys::nfs4::clientid4 id,
        const filesys::nfs4::client_owner4& owner,
        const std::string& principal,
        filesys::detail::Clock::time_point expiry,
        const filesys::nfs4::state_protect4_a& spa);

    ~NfsClient()
    {
        for (auto& entry: devices_) {
            auto dev = entry.first;
            auto& ds = entry.second;
            if (ds.callback)
                dev->removeStateCallback(ds.callback);
        }
    }

    auto id() const { return id_; }
    auto& owner() const { return owner_; }
    auto& principal() const { return principal_; }
    auto confirmed() const { return confirmed_; }
    auto sequence() const { return sequence_; }
    auto& reply() const { return reply_; }
    auto spa() const { return spa_; }

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

    auto& sessions() const { return sessions_; }

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
        std::shared_ptr<filesys::OpenFile> of)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto id = newStateId();
        auto ns = std::make_shared<NfsState>(
            NfsState::OPEN, id, shared_from_this(), fs,
            owner, access, deny, of);
        state_[id] = ns;
        return ns;
    }

    std::shared_ptr<NfsState> addDelegation(
        std::shared_ptr<NfsFileState> fs,
        const filesys::nfs4::state_owner4& owner,
        int access,
        std::shared_ptr<filesys::OpenFile> of)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto id = newStateId();
        auto ns = std::make_shared<NfsState>(
            NfsState::DELEGATION, id, shared_from_this(), fs,
            owner, access, 0, of);
        assert(state_.find(id) == state_.end());
        recallableStateCount_++;
        state_[id] = ns;
        return ns;
    }

    std::shared_ptr<NfsState> addLayout(
        std::shared_ptr<NfsFileState> fs,
        filesys::nfs4::layoutiomode4 iomode,
        const std::vector<std::shared_ptr<filesys::Device>>& devices)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto id = newStateId();
        int access = (iomode == filesys::nfs4::LAYOUTIOMODE4_RW ?
                      filesys::nfs4::OPEN4_SHARE_ACCESS_BOTH :
                      filesys::nfs4::OPEN4_SHARE_ACCESS_READ);
        auto ns = std::make_shared<NfsState>(
            NfsState::LAYOUT, id, shared_from_this(), fs,
            filesys::nfs4::state_owner4{id_, {}}, access, 0, nullptr);
        for (auto dev: devices) {
            auto& ds = devices_[dev];
            ds.layouts.insert(ns);
            checkDeviceState(lock, dev, ds);
        }
        ns->setDevices(devices);
        assert(state_.find(id) == state_.end());
        recallableStateCount_++;
        state_[id] = ns;
        return ns;
    }

    void clearState(const filesys::nfs4::stateid4& stateid)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto it = state_.find(stateid);
        if (it != state_.end()) {
            auto ns = it->second;
            if (!ns->revoked()) {
                auto fs = ns->fs();
                if (fs)
                    fs->revoke(it->second);
                if (ns->type() == NfsState::DELEGATION ||
                    ns->type() == NfsState::LAYOUT)
                    recallableStateCount_--;
                if (ns->type() == NfsState::LAYOUT)
                    clearLayout(lock, ns);
            }
            state_.erase(it);
        }
        else {
            it = revokedState_.find(stateid);
            if (it != revokedState_.end())
                revokedState_.erase(it);
        }
    }

    void clearLayouts()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        std::vector<filesys::nfs4::stateid4> stateids;
        for (auto& entry: state_) {
            if (entry.second->type() == NfsState::LAYOUT)
                stateids.push_back(entry.second->id());
        }
        lock.unlock();
        for (auto& stateid: stateids)
            clearState(stateid);
    }

    void revokeState(std::shared_ptr<NfsState> ns)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto fs = ns->fs();
        if (fs) {
            fs->revoke(ns);
        }
        if (ns->type() == NfsState::DELEGATION ||
            ns->type() == NfsState::LAYOUT)
            recallableStateCount_--;
        if (ns->type() == NfsState::LAYOUT)
            clearLayout(lock, ns);
        state_.erase(ns->id());
        revokedState_[ns->id()] = ns;
    }

    void revokeState()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        for (auto& e: state_) {
            auto ns = e.second;
            auto fs = ns->fs();
            if (fs) {
                fs->revoke(ns);
            }
            if (ns->type() == NfsState::DELEGATION ||
                ns->type() == NfsState::LAYOUT)
                recallableStateCount_--;
            if (ns->type() == NfsState::LAYOUT)
                clearLayout(lock, ns);
            revokedState_[ns->id()] = ns;
        }
        state_.clear();
    }

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

private:
    std::mutex mutex_;
    filesys::nfs4::clientid4 id_;
    filesys::nfs4::client_owner4 owner_;
    std::string principal_;
    filesys::nfs4::nfs_impl_id4 impl_;
    bool expired_ = false;
    filesys::detail::Clock::time_point expiry_;
    filesys::nfs4::state_protect_how4 spa_;
    bool confirmed_ = false;
    std::atomic_int nextSessionIndex_;
    std::unordered_set<std::shared_ptr<NfsSession>> sessions_;

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
