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

}
}
