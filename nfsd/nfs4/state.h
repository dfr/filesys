/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include <filesys/filesys.h>
#include <util/util.h>
#include "filesys/nfs4/nfs4proto.h"

namespace keyval {
class Namespace;
class Transaction;
}

namespace nfsd {
namespace nfs4 {

class NfsClient;
class NfsFileState;

enum class StateType: std::uint32_t {
    OPEN,
    DELEGATION,
    LAYOUT
};

// State objects are stored stored in the database using a key in the
// following format:
//
//     <fh><state type><client_owner4::co_ownerid>[<state_owner4::owner>]
//
// The state_owner4::owner portion is only present for For StateType::OPEN
//
// The value is an xdr encoded StateData structure.
struct StateKey
{
    typedef oncrpc::bounded_vector<
        std::uint8_t, filesys::nfs4::NFS4_OPAQUE_LIMIT> ownerTy;

    filesys::nfs4::nfs_fh4 fh;
    StateType type;
    ownerTy clientOwner;
    ownerTy stateOwner;
};

template <class XDR>
void xdr(oncrpc::RefType<StateKey, XDR> v, XDR* xdrs)
{
    xdr(v.fh, xdrs);
    xdr(oncrpc::RefType<std::uint32_t, XDR>(v.type), xdrs);
    xdr(v.clientOwner, xdrs);
    if (v.type == StateType::OPEN)
        xdr(v.stateOwner, xdrs);
}

struct StateData
{
    int access  = 0;           // share access
    int deny = 0;              // share deny
    uint64_t offset = 0;       // for layouts, the layout state offset
    uint64_t length = filesys::nfs4::NFS4_UINT64_MAX; // layout size
};

template <class XDR>
void xdr(oncrpc::RefType<StateData, XDR> v, XDR* xdrs)
{
    xdr(v.access, xdrs);
    xdr(v.deny, xdrs);
    xdr(v.offset, xdrs);
    xdr(v.length, xdrs);
}

class NfsState: public std::enable_shared_from_this<NfsState>
{
public:

    NfsState(
        std::shared_ptr<keyval::Namespace> stateNS,
        StateType type,
        const filesys::nfs4::stateid4& id,
        std::shared_ptr<NfsClient> client,
        std::shared_ptr<NfsFileState> fs,
        const filesys::nfs4::state_owner4& owner,
        int access,
        int deny,
        std::shared_ptr<filesys::OpenFile> of,
        util::Clock::time_point expiry);

    auto type() const { return type_; }
    auto id() const { return id_; }
    auto client() const { return client_.lock(); }
    auto fs() const { return fs_.lock(); }
    auto owner() const { return owner_; }
    auto access() const { return data_.access; }
    auto deny() const { return data_.deny; }
    auto offset() const { return data_.offset; }
    auto length() const { return data_.length; }
    auto devices() const { return devices_; }
    auto of() const { return of_; }
    auto restored() const { return restored_; }
    auto revoked() const { return revoked_; }
    auto recalled() const { return recalled_; }

    void save(keyval::Transaction* trans);
    void remove(keyval::Transaction* trans);

    /// Set the restored flag - states read from the database on
    /// startup are marked as restored until the corresponding client
    /// either reclaims them or calls OP_RECLAIM_COMPLETE. If the
    /// server is in its grace period, conflicting operations will
    /// return NFS4ERR_GRACE. If the server has completed its grace
    /// period, any conflicting operation will revoke the restored
    /// state.
    void setRestored()
    {
        restored_ = true;
    }

    /// Called when a restored state is reclaimed by its owning
    /// client. Clear the restored flag and set the OpenFile pointer.
    void setReclaimed(std::shared_ptr<filesys::OpenFile> of)
    {
        restored_ = false;
        of_ = of;
    }

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
        data_.offset = offset;
    }

    void setLength(std::uint64_t length)
    {
        data_.length = length;
    }

    void setDevices(const std::vector<std::shared_ptr<filesys::Device>>& devs)
    {
        devices_ = devs;
    }

    auto expiry() const { return expiry_; }
    void setExpiry(util::Clock::time_point expiry)
    {
        expiry_ = expiry;
    }

private:
    std::shared_ptr<keyval::Namespace> stateNS_;
    StateType type_;
    filesys::nfs4::stateid4 id_;
    std::weak_ptr<NfsClient> client_;
    std::weak_ptr<NfsFileState> fs_;
    filesys::nfs4::state_owner4 owner_;
    StateData data_;
    std::vector<std::shared_ptr<filesys::Device>> devices_;
    std::shared_ptr<filesys::OpenFile> of_;
    bool restored_ = false;
    bool revoked_ = false;
    bool recalled_ = false;
    util::Clock::time_point expiry_;
};

}
}
