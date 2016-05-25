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
        uint32_t cbProg);

    auto client() const { return client_.lock(); }
    auto& id() const { return id_; }
    filesys::nfs4::SEQUENCE4res sequence(
        CompoundState& state, const filesys::nfs4::SEQUENCE4args& args);
    void addChannel(
        std::shared_ptr<oncrpc::Channel> chan, int dir, bool use_rdma);
    bool validChannel(std::shared_ptr<oncrpc::Channel> chan);

    void callback(
        const std::string& tag,
        std::function<void(filesys::nfs4::CallbackRequestEncoder& enc)> args,
        std::function<void(filesys::nfs4::CallbackReplyDecoder& dec)> res);

    bool testBackchannel();

    void recall(
        const filesys::nfs4::stateid4& stateid,
        const filesys::nfs4::nfs_fh4& fh);

    void setRecallHook(
        std::function<void(
            const filesys::nfs4::stateid4&,
            const filesys::nfs4::nfs_fh4&)> hook)
    {
        recallHook_ = hook;
    }

private:
    enum BackChannelState {
        NONE,
        UNCHECKED,
        CHECKING,
        GOOD
    };

    std::mutex mutex_;
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
    std::function<void(const filesys::nfs4::stateid4&,
                       const filesys::nfs4::nfs_fh4&)> recallHook_;
};

class NfsFileState;

class NfsState: public std::enable_shared_from_this<NfsState>
{
public:
    enum StateType {
        OPEN,
        DELEGATION,
        LOCK,
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
    auto of() const { return of_; }
    auto revoked() const { return revoked_; }

    /// Revoke the state, dropping our reference to the OpenFile. This
    /// should only be called from NfsFileState::revoke which
    /// completes the work by forgetting its reference to the NfsState
    /// instance.
    void revoke();

    /// For a delegation, issue a recall using the session's back channel
    void recall(std::shared_ptr<NfsSession> session);

    /// Update the share modes for this state entry
    void updateOpen(
        int access, int deny, std::shared_ptr<filesys::OpenFile> of);

    /// Update delegation type
    void updateDelegation(
        int access,  std::shared_ptr<filesys::OpenFile> of);

private:
    StateType type_;
    filesys::nfs4::stateid4 id_;
    std::weak_ptr<NfsClient> client_;
    std::weak_ptr<NfsFileState> fs_;
    filesys::nfs4::state_owner4 owner_;
    int access_;
    int deny_;
    std::shared_ptr<filesys::OpenFile> of_;
    bool revoked_ = false;
    bool recalled_ = false;
};

/// Track per-file state such as open files, locks, delegations etc.
class NfsFileState
{
public:
    auto lock()
    {
        return std::unique_lock<std::mutex>(mutex_);
    }

    void addOpen(std::shared_ptr<NfsState> ns);

    void removeOpen(std::shared_ptr<NfsState> ns);

    void addDelegation(std::shared_ptr<NfsState> ns);

    void removeDelegation(std::shared_ptr<NfsState> ns);

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
        const filesys::nfs4::open_owner4& owner)
    {
        auto lk = lock();
        return findOpen(lk, owner);
    }

    std::shared_ptr<NfsState> findOpen(
        std::unique_lock<std::mutex>& lock,
        const filesys::nfs4::open_owner4& owner);

    std::shared_ptr<NfsState> findDelegation(
        const filesys::nfs4::open_owner4& owner)
    {
        auto lk = lock();
        return findDelegation(lk, owner);
    }

    std::shared_ptr<NfsState> findDelegation(
        std::unique_lock<std::mutex>& lock,
        const filesys::nfs4::open_owner4& owner);

    /// Return true if this owner can open with the given access and
    /// deny share reservation
    bool checkShare(
        const filesys::nfs4::open_owner4& owner, int access, int deny);

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
        return opens_.size() > 0 || delegations_.size() > 0;
    }

    auto access() const { return access_; }
    auto deny() const { return deny_; }
    auto& opens() const { return opens_; }
    auto& delegations() const { return delegations_; }

private:
    void revoke(
        std::unique_lock<std::mutex>& lock, std::shared_ptr<NfsState> ns);

    std::mutex mutex_;
    int access_ = 0;
    int deny_ = 0;
    std::unordered_set<std::shared_ptr<NfsState>> opens_;
    std::unordered_set<std::shared_ptr<NfsState>> delegations_;
};

class NfsClient: public std::enable_shared_from_this<NfsClient>
{
public:
    NfsClient(
        filesys::nfs4::clientid4 id,
        const filesys::nfs4::client_owner4& owner,
        const std::string& principal,
        filesys::detail::Clock::time_point expiry);

    auto id() const { return id_; }
    auto& owner() const { return owner_; }
    auto& principal() const { return principal_; }
    auto confirmed() const { return confirmed_; }
    auto sequence() const { return sequence_; }
    auto& reply() const { return reply_; }

    bool hasState() const
    {
        return state_.size() != 0;
    }

    bool hasUnrevokedState() const
    {
        return revokedStateCount_ < state_.size();
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

    void clearState(const filesys::nfs4::stateid4& stateid)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto it = state_.find(stateid);
        if (it != state_.end()) {
            auto fs = it->second->fs();
            if (fs)
                fs->revoke(it->second);
            state_.erase(it);
        }
    }

    void revokeState()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        for (auto& e: state_) {
            e.second->fs()->revoke(e.second);
            revokedStateCount_++;
        }
    }

    void incrementRevoked()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        revokedStateCount_++;
    }

    auto revokedStateCount() const { return revokedStateCount_; }

private:
    std::mutex mutex_;
    filesys::nfs4::clientid4 id_;
    filesys::nfs4::client_owner4 owner_;
    std::string principal_;
    filesys::nfs4::nfs_impl_id4 impl_;
    bool expired_ = false;
    filesys::detail::Clock::time_point expiry_;
    bool confirmed_ = false;
    std::atomic_int nextSessionIndex_;
    std::unordered_set<std::shared_ptr<NfsSession>> sessions_;

    // Current state
    std::atomic_int nextStateIndex_;
    std::unordered_map<
        filesys::nfs4::stateid4,
        std::shared_ptr<NfsState>,
        filesys::nfs4::NfsStateidHashIgnoreSeqid,
        filesys::nfs4::NfsStateidEqualIgnoreSeqid> state_;
    int revokedStateCount_ = 0;

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
            auto fs = std::make_shared<NfsFileState>();
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
