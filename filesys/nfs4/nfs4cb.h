/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include "nfs4proto.h"

namespace filesys {
namespace nfs4 {

class NfsFilesystem;

class INfsCallback
{
public:
    virtual ~INfsCallback() {}
    virtual void null() = 0;
    virtual CB_GETATTR4res getattr(
        const CB_GETATTR4args& args) = 0;
    virtual CB_RECALL4res recall(
        const CB_RECALL4args& args) = 0;
    virtual CB_LAYOUTRECALL4res layoutrecall(
        const CB_LAYOUTRECALL4args& args) = 0;
    virtual CB_NOTIFY4res notify(
        const CB_NOTIFY4args& args) = 0;
    virtual CB_PUSH_DELEG4res push_deleg(
        const CB_PUSH_DELEG4args& args) = 0;
    virtual CB_RECALL_ANY4res recall_any(
        const CB_RECALL_ANY4args& args) = 0;
    virtual CB_RECALLABLE_OBJ_AVAIL4res recallable_obj_avail(
        const CB_RECALLABLE_OBJ_AVAIL4args& args) = 0;
    virtual CB_RECALL_SLOT4res recall_slot(
        const CB_RECALL_SLOT4args& args) = 0;
    virtual CB_WANTS_CANCELLED4res wants_cancelled(
        const CB_WANTS_CANCELLED4args& args) = 0;
    virtual CB_NOTIFY_LOCK4res notify_lock(
        const CB_NOTIFY_LOCK4args& args) = 0;
    virtual CB_NOTIFY_DEVICEID4res notify_deviceid(
        const CB_NOTIFY_DEVICEID4args& args) = 0;
};

class NfsCallbackService: public INfsCallback
{
public:
    NfsCallbackService(NfsFilesystem* fs);

    void null() override;
    CB_GETATTR4res getattr(
        const CB_GETATTR4args& args) override;
    CB_RECALL4res recall(
        const CB_RECALL4args& args) override;
    CB_LAYOUTRECALL4res layoutrecall(
        const CB_LAYOUTRECALL4args& args) override;
    CB_NOTIFY4res notify(
        const CB_NOTIFY4args& args) override;
    CB_PUSH_DELEG4res push_deleg(
        const CB_PUSH_DELEG4args& args) override;
    CB_RECALL_ANY4res recall_any(
        const CB_RECALL_ANY4args& args) override;
    CB_RECALLABLE_OBJ_AVAIL4res recallable_obj_avail(
        const CB_RECALLABLE_OBJ_AVAIL4args& args) override;
    CB_RECALL_SLOT4res recall_slot(
        const CB_RECALL_SLOT4args& args) override;
    CB_WANTS_CANCELLED4res wants_cancelled(
        const CB_WANTS_CANCELLED4args& args) override;
    CB_NOTIFY_LOCK4res notify_lock(
        const CB_NOTIFY_LOCK4args& args) override;
    CB_NOTIFY_DEVICEID4res notify_deviceid(
        const CB_NOTIFY_DEVICEID4args& args) override;

    virtual void dispatch(oncrpc::CallContext&& ctx);
    void compound(oncrpc::CallContext&& ctx);
    nfsstat4 dispatchop(
        nfs_cb_opnum4 op, oncrpc::XdrSource* xargs, oncrpc::XdrSink* xresults);
    void bind(uint32_t prog, std::shared_ptr<oncrpc::ServiceRegistry> svcreg);
    void unbind(uint32_t prog, std::shared_ptr<oncrpc::ServiceRegistry> svcreg);

    void setSlots(int slots)
    {
        slots_.clear();
        for (int i = 0; i < slots; i++)
            slots_.emplace_back(Slot{0});
    }

private:
    struct Slot {
        sequenceid4 sequence;
        std::unique_ptr<oncrpc::Message> reply;
    };

private:
    NfsFilesystem* fs_;
    std::vector<Slot> slots_;
};

}
}
