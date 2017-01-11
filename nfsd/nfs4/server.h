/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include <filesys/filesys.h>
#include <rpc++/rest.h>

#include "filesys/nfs4/nfs4proto.h"
#include "filesys/nfs4/nfs4attr.h"
#include "filesys/nfs4/nfs4compound.h"
#include "filesys/nfs4/nfs4idmap.h"
#include "filesys/nfs4/nfs4util.h"

#include "client.h"
#include "filestate.h"

namespace keyval {
class Namespace;
}

namespace nfsd {
namespace nfs4 {

class NfsSession;

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

class NfsServer: public INfsServer, public oncrpc::RestHandler,
                 public std::enable_shared_from_this<NfsServer>
{
public:
    NfsServer(
        const std::vector<int>& sec,
        std::shared_ptr<filesys::Filesystem> fs,
        const std::vector<oncrpc::AddressInfo>& addrs,
        std::shared_ptr<filesys::nfs4::IIdMapper> idmapper,
        std::shared_ptr<util::Clock> clock);
    NfsServer(
        const std::vector<int>& sec,
        std::shared_ptr<filesys::Filesystem> fs,
        const std::vector<oncrpc::AddressInfo>& addrs);
    ~NfsServer();

    // INfsServer overrides
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

    // RestHandler overrides
    bool get(
        std::shared_ptr<oncrpc::RestRequest> req,
        std::unique_ptr<oncrpc::RestEncoder>&& res) override;

    auto clientsNS() const { return clientsNS_; }
    auto stateNS() const { return stateNS_; }

    virtual void dispatch(oncrpc::CallContext&& ctx);
    void compound(oncrpc::CallContext&& ctx);
    filesys::nfs4::nfsstat4 dispatchop(
        filesys::nfs4::nfs_opnum4 op, CompoundState& state,
        oncrpc::XdrSource* xargs, oncrpc::XdrSink* xresults);

    /// Return the time at which a client which renews its leases now
    /// should expire
    util::Clock::time_point leaseExpiry();

    /// Wrap db_->beginTransaction
    std::unique_ptr<keyval::Transaction> beginTransaction();

    /// Wrap db_->commit
    void commit(std::unique_ptr<keyval::Transaction>&& trans);

    /// Return true if client state is persistent
    bool persistentState() const {
        return db_ != nullptr;
    }

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

    /// Get a set of attributes, returning them in the given NfsAttr structure
    void getAttr(
        std::shared_ptr<filesys::File> file,
        const filesys::nfs4::bitmap4& wanted,
        filesys::nfs4::NfsAttr& xattr);

    /// Return a wire-format attribute set for the given attributes
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

    /// Set a REST registry object to allow access to the server state
    /// for monitoring and managment interfaces. This should be called
    /// on startup, before any clients are connected. Note: there may
    /// be client objects loaded from the DB so make sure we hook them
    /// up to the registry as well
    void setRestRegistry(std::shared_ptr<oncrpc::RestRegistry> restreg)
    {
        assert(!restreg_.lock());
        restreg_ = restreg;
        restreg->add("/nfs4", false, shared_from_this());
        for (auto& entry: clientsById_)
            entry.second->setRestRegistry(restreg);
    }

private:
    std::mutex mutex_;
    std::vector<int> sec_;
    std::shared_ptr<filesys::Filesystem> fs_;
    std::vector<oncrpc::AddressInfo> addrs_;
    std::shared_ptr<keyval::Database> db_;
    std::shared_ptr<keyval::Namespace> clientsNS_;
    std::shared_ptr<keyval::Namespace> stateNS_;
    filesys::nfs4::server_owner4 owner_;
    filesys::nfs4::verifier4 writeverf_;
    std::shared_ptr<filesys::nfs4::IIdMapper> idmapper_;
    std::shared_ptr<util::Clock> clock_;
    util::Clock::time_point graceExpiry_;
    std::weak_ptr<oncrpc::RestRegistry> restreg_;
    bool expiring_ = false;

    // Statistics
    std::vector<int> stats_;     // per-compound op counts

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
