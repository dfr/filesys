// -*- c++ -*-
#pragma once

#include <fs++/filesys.h>

#include "filesys/nfs4/nfs4proto.h"
#include "filesys/nfs4/nfs4attr.h"
#include "filesys/nfs4/nfs4compound.h"
#include "filesys/nfs4/nfs4idmap.h"
#include "filesys/nfs4/nfs4util.h"

namespace nfsd {
namespace nfs4 {

class NfsSession;

struct Slot
{
    bool busy = false;
    filesys::nfs4::sequenceid4 sequence = 0;
    std::unique_ptr<oncrpc::Message> reply;
};

struct CBSlot {
    bool busy = false;
    filesys::nfs4::sequenceid4 sequence = 1;
};

struct CompoundState
{
    Slot* slot = nullptr;
    std::shared_ptr<NfsSession> session;
    int opindex;
    int opcount;
    struct {
        std::shared_ptr<filesys::File> file;
        filesys::nfs4::stateid4 stateid = filesys::nfs4::STATEID_INVALID;
    } curr, save;
};

class INfsServer
{
public:
    virtual ~INfsServer() {}

    virtual void null() = 0;
    virtual filesys::nfs4::ACCESS4res access(
        CompoundState& state,
        const filesys::nfs4::ACCESS4args& args) = 0;
    virtual filesys::nfs4::CLOSE4res close(
        CompoundState& state,
        const filesys::nfs4::CLOSE4args& args) = 0;
    virtual filesys::nfs4::COMMIT4res commit(
        CompoundState& state,
        const filesys::nfs4::COMMIT4args& args) = 0;
    virtual filesys::nfs4::CREATE4res create(
        CompoundState& state,
        const filesys::nfs4::CREATE4args& args) = 0;
    virtual filesys::nfs4::DELEGPURGE4res delegpurge(
        CompoundState& state,
        const filesys::nfs4::DELEGPURGE4args& args) = 0;
    virtual filesys::nfs4::DELEGRETURN4res delegreturn(
        CompoundState& state,
        const filesys::nfs4::DELEGRETURN4args& args) = 0;
    virtual filesys::nfs4::GETATTR4res getattr(
        CompoundState& state,
        const filesys::nfs4::GETATTR4args& args) = 0;
    virtual filesys::nfs4::GETFH4res getfh(
        CompoundState& state) = 0;
    virtual filesys::nfs4::LINK4res link(
        CompoundState& state,
        const filesys::nfs4::LINK4args& args) = 0;
    virtual filesys::nfs4::LOCK4res lock(
        CompoundState& state,
        const filesys::nfs4::LOCK4args& args) = 0;
    virtual filesys::nfs4::LOCKT4res lockt(
        CompoundState& state,
        const filesys::nfs4::LOCKT4args& args) = 0;
    virtual filesys::nfs4::LOCKU4res locku(
        CompoundState& state,
        const filesys::nfs4::LOCKU4args& args) = 0;
    virtual filesys::nfs4::LOOKUP4res lookup(
        CompoundState& state,
        const filesys::nfs4::LOOKUP4args& args) = 0;
    virtual filesys::nfs4::LOOKUPP4res lookupp(
        CompoundState& state) = 0;
    virtual filesys::nfs4::NVERIFY4res nverify(
        CompoundState& state,
        const filesys::nfs4::NVERIFY4args& args) = 0;
    virtual filesys::nfs4::OPEN4res open(
        CompoundState& state,
        const filesys::nfs4::OPEN4args& args) = 0;
    virtual filesys::nfs4::OPENATTR4res openattr(
        CompoundState& state,
        const filesys::nfs4::OPENATTR4args& args) = 0;
    virtual filesys::nfs4::OPEN_CONFIRM4res open_confirm(
        CompoundState& state,
        const filesys::nfs4::OPEN_CONFIRM4args& args) = 0;
    virtual filesys::nfs4::OPEN_DOWNGRADE4res open_downgrade(
        CompoundState& state,
        const filesys::nfs4::OPEN_DOWNGRADE4args& args) = 0;
    virtual filesys::nfs4::PUTFH4res putfh(
        CompoundState& state,
        const filesys::nfs4::PUTFH4args& args) = 0;
    virtual filesys::nfs4::PUTPUBFH4res putpubfh(
        CompoundState& state) = 0;
    virtual filesys::nfs4::PUTROOTFH4res putrootfh(
        CompoundState& state) = 0;
    virtual filesys::nfs4::READ4res read(
        CompoundState& state,
        const filesys::nfs4::READ4args& args) = 0;
    virtual filesys::nfs4::READDIR4res readdir(
        CompoundState& state,
        const filesys::nfs4::READDIR4args& args) = 0;
    virtual filesys::nfs4::READLINK4res readlink(
        CompoundState& state) = 0;
    virtual filesys::nfs4::REMOVE4res remove(
        CompoundState& state,
        const filesys::nfs4::REMOVE4args& args) = 0;
    virtual filesys::nfs4::RENAME4res rename(
        CompoundState& state,
        const filesys::nfs4::RENAME4args& args) = 0;
    virtual filesys::nfs4::RENEW4res renew(
        CompoundState& state,
        const filesys::nfs4::RENEW4args& args) = 0;
    virtual filesys::nfs4::RESTOREFH4res restorefh(
        CompoundState& state) = 0;
    virtual filesys::nfs4::SAVEFH4res savefh(
        CompoundState& state) = 0;
    virtual filesys::nfs4::SECINFO4res secinfo(
        CompoundState& state,
        const filesys::nfs4::SECINFO4args& args) = 0;
    virtual filesys::nfs4::SETATTR4res setattr(
        CompoundState& state,
        const filesys::nfs4::SETATTR4args& args) = 0;
    virtual filesys::nfs4::SETCLIENTID4res setclientid(
        CompoundState& state,
        const filesys::nfs4::SETCLIENTID4args& args) = 0;
    virtual filesys::nfs4::SETCLIENTID_CONFIRM4res setclientid_confirm(
        CompoundState& state,
        const filesys::nfs4::SETCLIENTID_CONFIRM4args& args) = 0;
    virtual filesys::nfs4::VERIFY4res verify(
        CompoundState& state,
        const filesys::nfs4::VERIFY4args& args) = 0;
    virtual filesys::nfs4::WRITE4res write(
        CompoundState& state,
        const filesys::nfs4::WRITE4args& args) = 0;
    virtual filesys::nfs4::RELEASE_LOCKOWNER4res release_lockowner(
        CompoundState& state,
        const filesys::nfs4::RELEASE_LOCKOWNER4args& args) = 0;
    virtual filesys::nfs4::BACKCHANNEL_CTL4res backchannel_ctl(
        CompoundState& state,
        const filesys::nfs4::BACKCHANNEL_CTL4args& args) = 0;
    virtual filesys::nfs4::BIND_CONN_TO_SESSION4res bind_conn_to_session(
        CompoundState& state,
        const filesys::nfs4::BIND_CONN_TO_SESSION4args& args) = 0;
    virtual filesys::nfs4::EXCHANGE_ID4res exchange_id(
        CompoundState& state,
        const filesys::nfs4::EXCHANGE_ID4args& args) = 0;
    virtual filesys::nfs4::CREATE_SESSION4res create_session(
        CompoundState& state,
        const filesys::nfs4::CREATE_SESSION4args& args) = 0;
    virtual filesys::nfs4::DESTROY_SESSION4res destroy_session(
        CompoundState& state,
        const filesys::nfs4::DESTROY_SESSION4args& args) = 0;
    virtual filesys::nfs4::FREE_STATEID4res free_stateid(
        CompoundState& state,
        const filesys::nfs4::FREE_STATEID4args& args) = 0;
    virtual filesys::nfs4::GET_DIR_DELEGATION4res get_dir_delegation(
        CompoundState& state,
        const filesys::nfs4::GET_DIR_DELEGATION4args& args) = 0;
    virtual filesys::nfs4::GETDEVICEINFO4res getdeviceinfo(
        CompoundState& state,
        const filesys::nfs4::GETDEVICEINFO4args& args) = 0;
    virtual filesys::nfs4::GETDEVICELIST4res getdevicelist(
        CompoundState& state,
        const filesys::nfs4::GETDEVICELIST4args& args) = 0;
    virtual filesys::nfs4::LAYOUTCOMMIT4res layoutcommit(
        CompoundState& state,
        const filesys::nfs4::LAYOUTCOMMIT4args& args) = 0;
    virtual filesys::nfs4::LAYOUTGET4res layoutget(
        CompoundState& state,
        const filesys::nfs4::LAYOUTGET4args& args) = 0;
    virtual filesys::nfs4::LAYOUTRETURN4res layoutreturn(
        CompoundState& state,
        const filesys::nfs4::LAYOUTRETURN4args& args) = 0;
    virtual filesys::nfs4::SECINFO_NO_NAME4res secinfo_no_name(
        CompoundState& state,
        const filesys::nfs4::SECINFO_NO_NAME4args& args) = 0;
    virtual filesys::nfs4::SEQUENCE4res sequence(
        CompoundState& state,
        const filesys::nfs4::SEQUENCE4args& args) = 0;
    virtual filesys::nfs4::SET_SSV4res set_ssv(
        CompoundState& state,
        const filesys::nfs4::SET_SSV4args& args) = 0;
    virtual filesys::nfs4::TEST_STATEID4res test_stateid(
        CompoundState& state,
        const filesys::nfs4::TEST_STATEID4args& args) = 0;
    virtual filesys::nfs4::WANT_DELEGATION4res want_delegation(
        CompoundState& state,
        const filesys::nfs4::WANT_DELEGATION4args& args) = 0;
    virtual filesys::nfs4::DESTROY_CLIENTID4res destroy_clientid(
        CompoundState& state,
        const filesys::nfs4::DESTROY_CLIENTID4args& args) = 0;
    virtual filesys::nfs4::RECLAIM_COMPLETE4res reclaim_complete(
        CompoundState& state,
        const filesys::nfs4::RECLAIM_COMPLETE4args& args) = 0;
};

class NfsClient;

class NfsSession: public std::enable_shared_from_this<NfsSession>
{
public:
    NfsSession(
        std::shared_ptr<NfsClient> client,
        std::shared_ptr<oncrpc::Channel> chan,
        const filesys::nfs4::channel_attrs4& fca,
        const filesys::nfs4::channel_attrs4& bca,
        uint32_t cbProg,
        const std::vector<filesys::nfs4::callback_sec_parms4>& cbSec);

    auto client() const { return client_.lock(); }
    auto& id() const { return id_; }
    void setCallback(
        uint32_t cbProg,
        const std::vector<filesys::nfs4::callback_sec_parms4>& cbSec);

    filesys::nfs4::SEQUENCE4res sequence(
        CompoundState& state, const filesys::nfs4::SEQUENCE4args& args);
    void addChannel(
        std::shared_ptr<oncrpc::Channel> chan, int dir, bool use_rdma);
    bool validChannel(std::shared_ptr<oncrpc::Channel> chan);
    bool hasBackChannel() const { return bool(backChannel_.lock()); }

    void callback(
        const std::string& tag,
        std::function<void(filesys::nfs4::CallbackRequestEncoder& enc)> args,
        std::function<void(filesys::nfs4::CallbackReplyDecoder& dec)> res);

    bool testBackchannel();

    void recallDelegation(
        const filesys::nfs4::stateid4& stateid,
        const filesys::nfs4::nfs_fh4& fh);

    void recallLayout(
        filesys::nfs4::layouttype4 type,
        filesys::nfs4::layoutiomode4 iomode,
        bool changed,
        const filesys::nfs4::layoutrecall4& recall);

    void setRecallHook(
        std::function<void(
            const filesys::nfs4::stateid4&,
            const filesys::nfs4::nfs_fh4&)> hook)
    {
        recallHook_ = hook;
    }

    void setLayoutRecallHook(
        std::function<void(
            filesys::nfs4::layouttype4 type,
            filesys::nfs4::layoutiomode4 iomode,
            bool changed,
            const filesys::nfs4::layoutrecall4& recall)> hook)
    {
        layoutRecallHook_ = hook;
    }

private:
    enum BackChannelState {
        NONE,
        UNCHECKED,
        CHECKING,
        GOOD
    };

    std::mutex mutex_;
    std::vector<filesys::nfs4::callback_sec_parms4> callbackSec_;
    std::weak_ptr<NfsClient> client_;
    std::vector<std::weak_ptr<oncrpc::Channel>> channels_;
    std::weak_ptr<oncrpc::Channel> backChannel_;
    BackChannelState backChannelState_ = UNCHECKED;
    std::condition_variable backChannelWait_;
    filesys::nfs4::sessionid4 id_;
    std::vector<Slot> slots_;

    // Callback slot state
    std::shared_ptr<oncrpc::Client> cbClient_;
    std::vector<CBSlot> cbSlots_;
    int cbHighestSlot_;                // current highest slot in-use
    int targetCbHighestSlot_;          // target slot limit
    std::condition_variable cbSlotWait_; // wait for free slot

    // Hook recalls for unit testing
    std::function<void(
        const filesys::nfs4::stateid4&,
        const filesys::nfs4::nfs_fh4&)> recallHook_;
    std::function<void(
        filesys::nfs4::layouttype4 type,
        filesys::nfs4::layoutiomode4 iomode,
        bool changed,
        const filesys::nfs4::layoutrecall4& recall)> layoutRecallHook_;
};

class NfsFileState;

class NfsState: public std::enable_shared_from_this<NfsState>
{
public:
    enum StateType {
        OPEN,
        DELEGATION,
        LAYOUT
    };

    NfsState(
        StateType type,
        const filesys::nfs4::stateid4& id,
        std::shared_ptr<NfsClient> client,
        std::shared_ptr<NfsFileState> fs,
        const filesys::nfs4::state_owner4& owner,
        int access,
        int deny,
        std::shared_ptr<filesys::OpenFile> of)
        : type_(type),
          id_(id),
          client_(client),
          fs_(fs),
          owner_(owner),
          access_(access),
          deny_(deny),
          of_(of)
    {
    }

    auto type() const { return type_; }
    auto id() const { return id_; }
    auto client() const { return client_.lock(); }
    auto fs() const { return fs_.lock(); }
    auto owner() const { return owner_; }
    auto access() const { return access_; }
    auto deny() const { return deny_; }
    auto offset() const { return offset_; }
    auto length() const { return length_; }
    auto devices() const { return devices_; }
    auto of() const { return of_; }
    auto revoked() const { return revoked_; }

    /// Revoke the state, dropping our reference to the OpenFile. This
    /// should only be called from NfsFileState::revoke which
    /// completes the work by forgetting its reference to the NfsState
    /// instance.
    void revoke();

    /// For a delegation or layout, issue a recall using the back
    /// channel of some session of the owning client
    void recall();

    /// Increment a layout seqid, making sure we don't wrap to zero
    void updateLayout()
    {
        id_.seqid++;
        if (id_.seqid == 0) id_.seqid++;
    }

    /// Update the share modes for this state entry
    void updateOpen(
        int access, int deny, std::shared_ptr<filesys::OpenFile> of);

    /// Update delegation type
    void updateDelegation(
        int access,  std::shared_ptr<filesys::OpenFile> of);

    void setOffset(std::uint64_t offset)
    {
        offset_ = offset;
    }

    void setLength(std::uint64_t length)
    {
        length_ = length;
    }

    void setDevices(const std::vector<std::shared_ptr<filesys::Device>>& devs)
    {
        devices_ = devs;
    }

private:
    StateType type_;
    filesys::nfs4::stateid4 id_;
    std::weak_ptr<NfsClient> client_;
    std::weak_ptr<NfsFileState> fs_;
    filesys::nfs4::state_owner4 owner_;
    int access_;
    int deny_;
    std::uint64_t offset_ = 0;
    std::uint64_t length_ = filesys::nfs4::NFS4_UINT64_MAX;
    std::vector<std::shared_ptr<filesys::Device>> devices_;
    std::shared_ptr<filesys::OpenFile> of_;
    bool revoked_ = false;
    bool recalled_ = false;
};

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

    // Pseudo-slot for create_session
    filesys::nfs4::sequenceid4 sequence_;
    filesys::nfs4::CREATE_SESSION4res reply_;
};

class NfsServer: public INfsServer
{
public:
    NfsServer(
        const std::vector<int>& sec,
        std::shared_ptr<filesys::nfs4::IIdMapper> idmapper,
        std::shared_ptr<filesys::detail::Clock> clock);
    NfsServer(const std::vector<int>& sec);

    void null() override;
    filesys::nfs4::ACCESS4res access(
        CompoundState& state,
        const filesys::nfs4::ACCESS4args& args) override;
    filesys::nfs4::CLOSE4res close(
        CompoundState& state,
        const filesys::nfs4::CLOSE4args& args) override;
    filesys::nfs4::COMMIT4res commit(
        CompoundState& state,
        const filesys::nfs4::COMMIT4args& args) override;
    filesys::nfs4::CREATE4res create(
        CompoundState& state,
        const filesys::nfs4::CREATE4args& args) override;
    filesys::nfs4::DELEGPURGE4res delegpurge(
        CompoundState& state,
        const filesys::nfs4::DELEGPURGE4args& args) override;
    filesys::nfs4::DELEGRETURN4res delegreturn(
        CompoundState& state,
        const filesys::nfs4::DELEGRETURN4args& args) override;
    filesys::nfs4::GETATTR4res getattr(
        CompoundState& state,
        const filesys::nfs4::GETATTR4args& args) override;
    filesys::nfs4::GETFH4res getfh(
        CompoundState& state) override;
    filesys::nfs4::LINK4res link(
        CompoundState& state,
        const filesys::nfs4::LINK4args& args) override;
    filesys::nfs4::LOCK4res lock(
        CompoundState& state,
        const filesys::nfs4::LOCK4args& args) override;
    filesys::nfs4::LOCKT4res lockt(
        CompoundState& state,
        const filesys::nfs4::LOCKT4args& args) override;
    filesys::nfs4::LOCKU4res locku(
        CompoundState& state,
        const filesys::nfs4::LOCKU4args& args) override;
    filesys::nfs4::LOOKUP4res lookup(
        CompoundState& state,
        const filesys::nfs4::LOOKUP4args& args) override;
    filesys::nfs4::LOOKUPP4res lookupp(
        CompoundState& state) override;
    filesys::nfs4::NVERIFY4res nverify(
        CompoundState& state,
        const filesys::nfs4::NVERIFY4args& args) override;
    filesys::nfs4::OPEN4res open(
        CompoundState& state,
        const filesys::nfs4::OPEN4args& args) override;
    filesys::nfs4::OPENATTR4res openattr(
        CompoundState& state,
        const filesys::nfs4::OPENATTR4args& args) override;
    filesys::nfs4::OPEN_CONFIRM4res open_confirm(
        CompoundState& state,
        const filesys::nfs4::OPEN_CONFIRM4args& args) override;
    filesys::nfs4::OPEN_DOWNGRADE4res open_downgrade(
        CompoundState& state,
        const filesys::nfs4::OPEN_DOWNGRADE4args& args) override;
    filesys::nfs4::PUTFH4res putfh(
        CompoundState& state,
        const filesys::nfs4::PUTFH4args& args) override;
    filesys::nfs4::PUTPUBFH4res putpubfh(
        CompoundState& state) override;
    filesys::nfs4::PUTROOTFH4res putrootfh(
        CompoundState& state) override;
    filesys::nfs4::READ4res read(
        CompoundState& state,
        const filesys::nfs4::READ4args& args) override;
    filesys::nfs4::READDIR4res readdir(
        CompoundState& state,
        const filesys::nfs4::READDIR4args& args) override;
    filesys::nfs4::READLINK4res readlink(
        CompoundState& state) override;
    filesys::nfs4::REMOVE4res remove(
        CompoundState& state,
        const filesys::nfs4::REMOVE4args& args) override;
    filesys::nfs4::RENAME4res rename(
        CompoundState& state,
        const filesys::nfs4::RENAME4args& args) override;
    filesys::nfs4::RENEW4res renew(
        CompoundState& state,
        const filesys::nfs4::RENEW4args& args) override;
    filesys::nfs4::RESTOREFH4res restorefh(
        CompoundState& state) override;
    filesys::nfs4::SAVEFH4res savefh(
        CompoundState& state) override;
    filesys::nfs4::SECINFO4res secinfo(
        CompoundState& state,
        const filesys::nfs4::SECINFO4args& args) override;
    filesys::nfs4::SETATTR4res setattr(
        CompoundState& state,
        const filesys::nfs4::SETATTR4args& args) override;
    filesys::nfs4::SETCLIENTID4res setclientid(
        CompoundState& state,
        const filesys::nfs4::SETCLIENTID4args& args) override;
    filesys::nfs4::SETCLIENTID_CONFIRM4res setclientid_confirm(
        CompoundState& state,
        const filesys::nfs4::SETCLIENTID_CONFIRM4args& args) override;
    filesys::nfs4::VERIFY4res verify(
        CompoundState& state,
        const filesys::nfs4::VERIFY4args& args) override;
    filesys::nfs4::WRITE4res write(
        CompoundState& state,
        const filesys::nfs4::WRITE4args& args) override;
    filesys::nfs4::RELEASE_LOCKOWNER4res release_lockowner(
        CompoundState& state,
        const filesys::nfs4::RELEASE_LOCKOWNER4args& args) override;
    filesys::nfs4::BACKCHANNEL_CTL4res backchannel_ctl(
        CompoundState& state,
        const filesys::nfs4::BACKCHANNEL_CTL4args& args) override;
    filesys::nfs4::BIND_CONN_TO_SESSION4res bind_conn_to_session(
        CompoundState& state,
        const filesys::nfs4::BIND_CONN_TO_SESSION4args& args) override;
    filesys::nfs4::EXCHANGE_ID4res exchange_id(
        CompoundState& state,
        const filesys::nfs4::EXCHANGE_ID4args& args) override;
    filesys::nfs4::CREATE_SESSION4res create_session(
        CompoundState& state,
        const filesys::nfs4::CREATE_SESSION4args& args) override;
    filesys::nfs4::DESTROY_SESSION4res destroy_session(
        CompoundState& state,
        const filesys::nfs4::DESTROY_SESSION4args& args) override;
    filesys::nfs4::FREE_STATEID4res free_stateid(
        CompoundState& state,
        const filesys::nfs4::FREE_STATEID4args& args) override;
    filesys::nfs4::GET_DIR_DELEGATION4res get_dir_delegation(
        CompoundState& state,
        const filesys::nfs4::GET_DIR_DELEGATION4args& args) override;
    filesys::nfs4::GETDEVICEINFO4res getdeviceinfo(
        CompoundState& state,
        const filesys::nfs4::GETDEVICEINFO4args& args) override;
    filesys::nfs4::GETDEVICELIST4res getdevicelist(
        CompoundState& state,
        const filesys::nfs4::GETDEVICELIST4args& args) override;
    filesys::nfs4::LAYOUTCOMMIT4res layoutcommit(
        CompoundState& state,
        const filesys::nfs4::LAYOUTCOMMIT4args& args) override;
    filesys::nfs4::LAYOUTGET4res layoutget(
        CompoundState& state,
        const filesys::nfs4::LAYOUTGET4args& args) override;
    filesys::nfs4::LAYOUTRETURN4res layoutreturn(
        CompoundState& state,
        const filesys::nfs4::LAYOUTRETURN4args& args) override;
    filesys::nfs4::SECINFO_NO_NAME4res secinfo_no_name(
        CompoundState& state,
        const filesys::nfs4::SECINFO_NO_NAME4args& args) override;
    filesys::nfs4::SEQUENCE4res sequence(
        CompoundState& state,
        const filesys::nfs4::SEQUENCE4args& args) override;
    filesys::nfs4::SET_SSV4res set_ssv(
        CompoundState& state,
        const filesys::nfs4::SET_SSV4args& args) override;
    filesys::nfs4::TEST_STATEID4res test_stateid(
        CompoundState& state,
        const filesys::nfs4::TEST_STATEID4args& args) override;
    filesys::nfs4::WANT_DELEGATION4res want_delegation(
        CompoundState& state,
        const filesys::nfs4::WANT_DELEGATION4args& args) override;
    filesys::nfs4::DESTROY_CLIENTID4res destroy_clientid(
        CompoundState& state,
        const filesys::nfs4::DESTROY_CLIENTID4args& args) override;
    filesys::nfs4::RECLAIM_COMPLETE4res reclaim_complete(
        CompoundState& state,
        const filesys::nfs4::RECLAIM_COMPLETE4args& args) override;

    virtual void dispatch(oncrpc::CallContext&& ctx);
    void compound(oncrpc::CallContext&& ctx);
    filesys::nfs4::nfsstat4 dispatchop(
        filesys::nfs4::nfs_opnum4 op, CompoundState& state,
        oncrpc::XdrSource* xargs, oncrpc::XdrSink* xresults);

    /// Return the time at which a client which renews its leases now
    /// should expire
    filesys::detail::Clock::time_point leaseExpiry();

    /// Return true if the server is still in its grace period
    bool inGracePeriod() const {
        return clock_->now() < graceExpiry_;
    }

    /// Expiry processing - typically called once per second. Returns
    /// the number of clients destroyed
    int expireClients();

    /// Destroy a client - the NfsServer mutex must be locked
    void destroyClient(
        std::unique_lock<std::mutex>& lock,
        std::shared_ptr<NfsClient> client);

    /// Drop the NfsFileState if it has no associated NfsState entries
    void checkState(std::shared_ptr<filesys::File> file)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto p = files_.find(file);
        if (p != files_.end()) {
            if (!p->second->hasState())
                files_.erase(p);
        }
    }

    std::shared_ptr<NfsFileState> findState(
        std::shared_ptr<filesys::File> file)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return findState(lock, file);
    }

    std::shared_ptr<NfsFileState> findState(
        std::unique_lock<std::mutex>& lock,
        std::shared_ptr<filesys::File> file)
    {
        auto p = files_.find(file);
        if (p == files_.end()) {
            auto fs = std::make_shared<NfsFileState>(file);
            files_[file] = fs;
            return fs;
        }
        return p->second;
    }

    /// Return a callable which sets supported attributes from
    /// attr. As a side effect of executing the callable, attrsset is
    /// changed to reflect which attributes were actually set.
    std::function<void(filesys::Setattr*)> importAttr(
        const filesys::nfs4::fattr4& attr,
        filesys::nfs4::bitmap4& attrsset);

    /// Return a wire-format atttribute set for the given attributes
    filesys::nfs4::fattr4 exportAttr(
        std::shared_ptr<filesys::File> file,
        const filesys::nfs4::bitmap4& wanted);

    /// Compare file attributes with the given set, returning NFS4ERR_SAME
    /// or NFS4ERR_NOT_SAME
    filesys::nfs4::nfsstat4 verifyAttr(
        std::shared_ptr<filesys::File> file,
        const filesys::nfs4::fattr4& check);

    /// Hook a session's recall messages for unit testing
    void setRecallHook(
        const filesys::nfs4::sessionid4& sessionid,
        std::function<void(const filesys::nfs4::stateid4& stateid,
                           const filesys::nfs4::nfs_fh4&)> hook);
    void setLayoutRecallHook(
        const filesys::nfs4::sessionid4& sessionid,
        std::function<void(
            filesys::nfs4::layouttype4 type,
            filesys::nfs4::layoutiomode4 iomode,
            bool changed,
            const filesys::nfs4::layoutrecall4& recall)> hook);

private:
    std::mutex mutex_;
    std::vector<int> sec_;
    filesys::nfs4::server_owner4 owner_;
    filesys::nfs4::verifier4 writeverf_;
    std::shared_ptr<filesys::nfs4::IIdMapper> idmapper_;
    std::shared_ptr<filesys::detail::Clock> clock_;
    filesys::detail::Clock::time_point graceExpiry_;

    // Active clients
    std::unordered_multimap<
        filesys::nfs4::NfsOwnerId,
        std::shared_ptr<NfsClient>,
        filesys::nfs4::NfsOwnerIdHash> clientsByOwnerId_;
    std::unordered_map<filesys::nfs4::clientid4,
        std::shared_ptr<NfsClient>> clientsById_;

    // Active sessions
    std::unordered_map<
        filesys::nfs4::sessionid4,
        std::shared_ptr<NfsSession>,
        filesys::nfs4::NfsSessionIdHash> sessionsById_;

    // Files with active state
    std::unordered_map<
        std::shared_ptr<filesys::File>,
        std::shared_ptr<NfsFileState>> files_;

    filesys::nfs4::clientid4 nextClientId_;
};

}
}
