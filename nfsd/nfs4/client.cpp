/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <glog/logging.h>

#include "client.h"
#include "session.h"
#include "util.h"

using namespace filesys;
using namespace filesys::nfs4;
using namespace nfsd::nfs4;
using namespace std;

DEFINE_int32(max_state, 0, "Target maximum number of recallable state objects per client (0 for unlimited)");

NfsClient::NfsClient(
    clientid4 id, const client_owner4& owner,
    const string& principal,
    detail::Clock::time_point expiry,
    const state_protect4_a& spa)
    : id_(id),
      owner_(owner),
      principal_(principal),
      expiry_(expiry),
      spa_(spa.spa_how),
      nextSessionIndex_(0),
      nextStateIndex_(0)
{
    sequence_ = 0;
    reply_ = CREATE_SESSION4res(NFS4ERR_SEQ_MISORDERED);
}

sessionid4 NfsClient::newSessionId()
{
    sessionid4 res;

    oncrpc::XdrMemory xm(res.data(), res.size());
    xdr(id_, static_cast<oncrpc::XdrSink*>(&xm));
    uint64_t index = ++nextSessionIndex_;
    xdr(index, static_cast<oncrpc::XdrSink*>(&xm));

    return res;
}

stateid4 NfsClient::newStateId()
{
    stateid4 res;

    res.seqid = 1;
    oncrpc::XdrMemory xm(res.other.data(), res.other.size());
    xdr(id_, static_cast<oncrpc::XdrSink*>(&xm));
    uint32_t index = ++nextStateIndex_;
    xdr(index, static_cast<oncrpc::XdrSink*>(&xm));

    return res;
}

shared_ptr<NfsState> NfsClient::findState(
    const CompoundState& state,
    const stateid4& stateid,
    bool allowRevoked)
{
    unique_lock<mutex> lock(mutex_);

    const stateid4& id =
        stateid == STATEID_LAST ? state.curr.stateid : stateid;

    auto it = state_.find(id);
    if (it == state_.end()) {
        if (allowRevoked) {
            it = revokedState_.find(id);
            if (it == revokedState_.end())
                throw NFS4ERR_BAD_STATEID;
        }
        else {
            throw NFS4ERR_BAD_STATEID;
        }
    }

    auto ns = it->second;
    if (id.seqid == 0 || id.seqid == ns->id().seqid)
        return ns;
    if (id.seqid > ns->id().seqid)
        throw NFS4ERR_BAD_STATEID;
    throw NFS4ERR_OLD_STATEID;
}

void NfsClient::setReply(const CREATE_SESSION4res& reply)
{
    unique_lock<mutex> lock(mutex_);
    sequence_++;
    if (reply.csr_status == NFS4_OK) {
        auto resok = reply.csr_resok4();
        reply_ = CREATE_SESSION4res(NFS4_OK, move(resok));
    }
    else {
        reply_ = CREATE_SESSION4res(reply.csr_status);
    }
}

void NfsClient::deviceCallback(shared_ptr<Device> dev, Device::State state)
{
    shared_ptr<NfsSession> session;
    for (auto s: sessions_) {
        if (s->hasBackChannel()) {
            session = s;
            break;
        }
    }
    if (!session) {
        LOG(ERROR) << "No back channel";
        return;
    }

    auto devid = dev->id();
    auto& ds = devices_[dev];
    switch (state) {
    case Device::ADDRESS_CHANGED:
        if (ds.notifications & (1 << NOTIFY_DEVICEID4_CHANGE)) {
            LOG(INFO) << "Client " << hex << id_
                      << ": notify change for device " << devid;
            session->callback(
                "DeviceChange",
                [=](auto& enc) {
                    notify_deviceid_change4 ndc;
                    ndc.ndc_layouttype = LAYOUT4_FLEX_FILES;
                    ndc.ndc_deviceid = exportDeviceid(devid);
                    ndc.ndc_immediate = true;

                    notify4 n;
                    set(n.notify_mask, NOTIFY_DEVICEID4_CHANGE);
                    n.notify_vals.resize(oncrpc::XdrSizeof(ndc));
                    oncrpc::XdrMemory xm(
                        n.notify_vals.data(), n.notify_vals.size());
                    xdr(ndc, static_cast<oncrpc::XdrSink*>(&xm));
                    enc.notify_deviceid({n});
                },
                [](auto& dec) {
                    dec.notify_deviceid();
                });
        }
        break;

    case Device::MISSING: {
        // We need to take a copy of layouts here since recall could delete
        // the layout if the client returns an error
        auto layouts = ds.layouts;
        for (auto ns: layouts)
            ns->recall();
        break;
    }

    case Device::DEAD:
        if (ds.notifications & (1 << NOTIFY_DEVICEID4_DELETE)) {
            LOG(INFO) << "Client " << hex << id_
                      << ": notify delete for device " << devid;
            session->callback(
                "DeviceDelete",
                [=](auto& enc) {
                    notify_deviceid_delete4 ndd;
                    ndd.ndd_layouttype = LAYOUT4_FLEX_FILES;
                    ndd.ndd_deviceid = exportDeviceid(devid);

                    notify4 n;
                    set(n.notify_mask, NOTIFY_DEVICEID4_DELETE);
                    n.notify_vals.resize(oncrpc::XdrSizeof(ndd));
                    oncrpc::XdrMemory xm(
                        n.notify_vals.data(), n.notify_vals.size());
                    xdr(ndd, static_cast<oncrpc::XdrSink*>(&xm));
                    enc.notify_deviceid({n});
                },
                [](auto& dec) {
                    dec.notify_deviceid();
                });
        }
        break;

    default:
        break;
    }
}

void NfsClient::sendRecallAny()
{
    if (FLAGS_max_state == 0 || recallableStateCount_ <= FLAGS_max_state)
        return;

    shared_ptr<NfsSession> session;
    for (auto s: sessions_) {
        if (s->hasBackChannel()) {
            session = s;
            break;
        }
    }
    if (!session) {
        LOG(ERROR) << "No back channel";
        return;
    }
    
    VLOG(1) << "recallable state count: " << recallableStateCount_
            << ": sending CB_RECALL_ANY";
    session->callback(
        "RecallAny",
        [=](auto& enc) {
            bitmap4 mask;
            set(mask, RCA4_TYPE_MASK_RDATA_DLG);
            set(mask, RCA4_TYPE_MASK_WDATA_DLG);
            // XXX: Add flex files bits
            enc.recall_any(FLAGS_max_state * 3 / 4, mask);
        },
        [](auto& dec) {
            dec.recall_any();
        });
}
