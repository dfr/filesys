/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include "filesys/nfs4/nfs4proto.h"
#include "filesys/nfs4/nfs4compound.h"

namespace nfsd {
namespace nfs4 {

struct CompoundState;
class NfsClient;

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

class NfsSession: public oncrpc::RestHandler,
                  public std::enable_shared_from_this<NfsSession>
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

    // RestHandler overrides
    bool get(
        std::shared_ptr<oncrpc::RestRequest> req,
        std::unique_ptr<oncrpc::RestEncoder>&& res) override;

    void setRestRegistry(std::shared_ptr<oncrpc::RestRegistry> restreg);

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
    std::weak_ptr<oncrpc::RestRegistry> restreg_;

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

}
}
