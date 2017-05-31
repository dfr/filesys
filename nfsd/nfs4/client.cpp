/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#include <rpc++/json.h>
#include <rpc++/urlparser.h>
#include <rpc++/xdr.h>
#include <keyval/keyval.h>
#include <glog/logging.h>

#include "client.h"
#include "session.h"
#include "util.h"

using namespace filesys;
using namespace filesys::nfs4;
using namespace nfsd::nfs4;
using namespace std;

DEFINE_int32(max_state, 0,
             "Target maximum number of recallable state objects per client"
             " (0 for unlimited)");

NfsClient::NfsClient(
    shared_ptr<keyval::Database> db,
    clientid4 id, const client_owner4& owner,
    const string& principal,
    util::Clock::time_point expiry,
    const state_protect4_a& spa)
    : db_(db),
      id_(id),
      data_({owner, principal, spa.spa_how}),
      expiry_(expiry),
      nextSessionIndex_(0),
      nextStateIndex_(0)
{
    if (db_) {
        clientsNS_ = db->getNamespace("clients");
        stateNS_ = db->getNamespace("state");
    }
    sequence_ = 0;
    reply_ = CREATE_SESSION4res(NFS4ERR_SEQ_MISORDERED);
}

NfsClient::NfsClient(
    shared_ptr<keyval::Database> db,
    filesys::nfs4::clientid4 id,
    std::unique_ptr<keyval::Iterator>& iterator,
    util::Clock::time_point expiry)
    : db_(db),
      clientsNS_(db->getNamespace("clients")),
      stateNS_(db->getNamespace("state")),
      id_(id),
      expiry_(expiry),
      nextSessionIndex_(0),
      nextStateIndex_(0)
{
    auto val = iterator->value();
    oncrpc::XdrMemory xm(val->data(), val->size());
    xdr(data_, static_cast<oncrpc::XdrSource*>(&xm));
}

NfsClient::~NfsClient()
{
    auto restreg = restreg_.lock();
    if (restreg) {
        restreg->remove(std::string("/nfs4/client/") + toHexClientid(id_));
        restreg_.reset();
    }

    for (auto& entry: devices_) {
        auto dev = entry.first;
        auto& ds = entry.second;
        if (ds.callback)
            dev->removeStateCallback(ds.callback);
    }
}

bool NfsClient::get(
    std::shared_ptr<oncrpc::RestRequest> req,
    std::unique_ptr<oncrpc::RestEncoder>&& res)
{
    auto& uri = req->uri();
    if (uri.segments.size() == 3) {
        // /nfs4/client/<id>
        auto enc = res->object();
        enc->field("states")->number(int(state_.size()));
        enc->field("confirmed")->boolean(confirmed_);
        auto sessions = enc->field("sessions")->array();
        for (auto sp: sessions_) {
            sessions->element()->string(toHexSessionid(sp->id()));
        }
        sessions.reset();
        auto opens = enc->field("opens")->array();
        for (auto ns: orderedState_) {
            if (ns->type() == StateType::OPEN)
                opens->element()->string(toHexStateid(ns->id()));
        }
        opens.reset();
        auto delegations = enc->field("delegations")->array();
        for (auto ns: orderedState_) {
            if (ns->type() == StateType::DELEGATION)
                delegations->element()->string(toHexStateid(ns->id()));
        }
        delegations.reset();
        auto layouts = enc->field("layouts")->array();
        for (auto ns: orderedState_) {
            if (ns->type() == StateType::LAYOUT)
                layouts->element()->string(toHexStateid(ns->id()));
        }
        layouts.reset();
        enc.reset();
        return true;
    }
    else if (uri.segments.size() == 5 && uri.segments[3] == "state") {
        // /nfs4/client/<id>/state/<id>
        try {
            auto id = fromHexStateid(uri.segments[4]);
            auto it = state_.find(id);
            if (it == state_.end())
                return false;
            auto ns = *it->second;
            encodeState(move(res), ns);
            return true;
        }
        catch (system_error& e) {
            return false;
        }
    }
    return false;
}

bool NfsClient::post(
    std::shared_ptr<oncrpc::RestRequest> req,
    std::unique_ptr<oncrpc::RestEncoder>&& res)
{
    auto& uri = req->uri();
    if (uri.segments.size() == 4 && uri.segments[3] == "revoke") {
        LOG(INFO) << req->body();
        if (req->body() == "true") {
            revokeState();
        }
        return true;
    }
    return false;
}

void NfsClient::encodeState(
    std::unique_ptr<oncrpc::RestEncoder>&& enc,
    std::shared_ptr<NfsState> ns)
{
    auto obj = enc->object();
    obj->field("id")->string(toHexStateid(ns->id()));
    obj->field("seqid")->number(int(ns->id().seqid));
    if (ns->type() == StateType::OPEN) {
        obj->field("expiry")->string("Not applicable");
    }
    else {
        auto t = chrono::system_clock::to_time_t(ns->expiry());
        ostringstream ss;
        auto tm = *gmtime(&t);
        ss << put_time(&tm, "%a, %d %b %Y %H:%M:%S GMT");
        obj->field("expiry")->string(ss.str());
    }
    if (ns->of()) {
        obj->field("fh")->string(
            toHexFileHandle(exportFileHandle(ns->of()->file())));
    }
    obj->field("revoked")->boolean(ns->revoked());
    obj->field("access")->number(ns->access());
    obj->field("deny")->number(ns->deny());
}

void NfsClient::setRestRegistry(std::shared_ptr<oncrpc::RestRegistry> restreg)
{
    assert(!restreg_.lock());
    restreg_ = restreg;
    restreg->add(
        std::string("/nfs4/client/") + toHexClientid(id_), false,
        shared_from_this());
}

void NfsClient::save(keyval::Transaction* trans)
{
    if (trans) {
        auto key = make_shared<oncrpc::Buffer>(data_.owner.co_ownerid.size());
        copy_n(data_.owner.co_ownerid.data(),
               data_.owner.co_ownerid.size(), key->data());
        auto value = make_shared<oncrpc::Buffer>(oncrpc::XdrSizeof(data_));
        oncrpc::XdrMemory xm(value->data(), value->size());
        xdr(data_, static_cast<oncrpc::XdrSink*>(&xm));
        trans->put(clientsNS_, key, value);
    }
}

void NfsClient::remove(keyval::Transaction* trans)
{
    if (trans) {
        auto key = make_shared<oncrpc::Buffer>(data_.owner.co_ownerid.size());
        copy_n(data_.owner.co_ownerid.data(),
               data_.owner.co_ownerid.size(), key->data());
        trans->remove(clientsNS_, key);
    }
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
    shared_ptr<NfsState> ns;
    if (it == state_.end()) {
        if (allowRevoked) {
            auto it2 = revokedState_.find(id);
            if (it2 == revokedState_.end())
                throw NFS4ERR_BAD_STATEID;
            ns = it2->second;
        }
        else {
            throw NFS4ERR_BAD_STATEID;
        }
    }
    else {
        ns = *it->second;
    }
    if (id.seqid == 0 || id.seqid == ns->id().seqid)
        return ns;
    if (id.seqid > ns->id().seqid)
        throw NFS4ERR_BAD_STATEID;
    VLOG(1) << "old seqid, expected: " << ns->id().seqid
            << ", given: " << id.seqid;
    throw NFS4ERR_OLD_STATEID;
}

shared_ptr<NfsState> NfsClient::addOpen(
    shared_ptr<NfsFileState> fs,
    const filesys::nfs4::state_owner4& owner,
    int access,
    int deny,
    shared_ptr<filesys::OpenFile> of,
    util::Clock::time_point expiry)
{
    unique_lock<mutex> lock(mutex_);
    auto id = newStateId();
    auto ns = make_shared<NfsState>(
        stateNS_, StateType::OPEN, id, shared_from_this(), fs,
        owner, access, deny, of, expiry);
    auto p = orderedState_.insert(orderedState_.end(), ns);
    state_[id] = p;
    fs->addOpen(ns);
    return ns;
}

shared_ptr<NfsState> NfsClient::addDelegation(
    shared_ptr<NfsFileState> fs,
    int access,
    shared_ptr<filesys::OpenFile> of,
    util::Clock::time_point expiry)
{
    unique_lock<mutex> lock(mutex_);
    auto id = newStateId();
    auto ns = make_shared<NfsState>(
        stateNS_, StateType::DELEGATION, id, shared_from_this(), fs,
        filesys::nfs4::state_owner4{id_, {}}, access, 0, of, expiry);
    assert(state_.find(id) == state_.end());
    recallableStateCount_++;
    auto p = orderedState_.insert(orderedState_.end(), ns);
    state_[id] = p;
    fs->addDelegation(ns);
    return ns;
}

shared_ptr<NfsState> NfsClient::addLayout(
    shared_ptr<NfsFileState> fs,
    filesys::nfs4::layoutiomode4 iomode,
    const vector<shared_ptr<filesys::Device>>& devices,
    util::Clock::time_point expiry)
{
    unique_lock<mutex> lock(mutex_);
    auto id = newStateId();
    int access = (iomode == filesys::nfs4::LAYOUTIOMODE4_RW ?
                  filesys::nfs4::OPEN4_SHARE_ACCESS_BOTH :
                  filesys::nfs4::OPEN4_SHARE_ACCESS_READ);
    auto ns = make_shared<NfsState>(
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
    auto p = orderedState_.insert(orderedState_.end(), ns);
    state_[id] = p;
    fs->addLayout(ns);
    return ns;
}

void NfsClient::clearState()
{
    revokeState();
    std::unique_lock<std::mutex> lock(mutex_);
    revokedState_.clear();
    recallableStateCount_ = 0;
}

void NfsClient::clearState(const filesys::nfs4::stateid4& stateid)
{
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = state_.find(stateid);
    if (it != state_.end()) {
        auto p = it->second;
        auto ns = *p;
        if (!ns->revoked()) {
            auto fs = ns->fs();
            if (fs)
                fs->revoke(ns);
            if (ns->type() == StateType::DELEGATION ||
                ns->type() == StateType::LAYOUT)
                recallableStateCount_--;
            if (ns->type() == StateType::LAYOUT)
                clearLayout(lock, ns);
        }
        state_.erase(it);
        orderedState_.erase(p);
    }
    else {
        auto it = revokedState_.find(stateid);
        if (it != revokedState_.end()) {
            revokedState_.erase(it);
        }
    }
}

void NfsClient::clearLayouts()
{
    std::unique_lock<std::mutex> lock(mutex_);
    std::vector<filesys::nfs4::stateid4> stateids;
    for (auto& entry: state_) {
        auto ns = *entry.second;
        if (ns->type() == StateType::LAYOUT)
            stateids.push_back(ns->id());
    }
    lock.unlock();
    for (auto& stateid: stateids)
        clearState(stateid);
}

void NfsClient::revokeState(std::shared_ptr<NfsState> ns)
{
    if (db_ && db_->isMaster()) {
        auto trans = db_->beginTransaction();
        revokeState(ns, trans.get());
        db_->commit(move(trans));
    }
    else {
        revokeState(ns, nullptr);
    }
}

void NfsClient::revokeState(
    std::shared_ptr<NfsState> ns, keyval::Transaction* trans)
{
    if (ns->revoked())
        return;

    ns->remove(trans);

    std::unique_lock<std::mutex> lock(mutex_);
    auto fs = ns->fs();
    if (fs) {
        fs->revoke(ns);
    }
    if (ns->type() == StateType::DELEGATION ||
        ns->type() == StateType::LAYOUT)
        recallableStateCount_--;
    if (ns->type() == StateType::LAYOUT)
        clearLayout(lock, ns);

    auto it = state_.find(ns->id());
    assert(it != state_.end());
    auto p = it->second;
    state_.erase(it);
    revokedState_[ns->id()] = *p;
    orderedState_.erase(p);
}

void NfsClient::revokeState()
{
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto& e: state_) {
        auto p = e.second;
        auto ns = *p;
        if (db_ && db_->isMaster()) {
            auto trans = db_->beginTransaction();
            ns->remove(trans.get());
            db_->commit(move(trans));
        }

        auto fs = ns->fs();
        if (fs) {
            fs->revoke(ns);
        }
        if (ns->type() == StateType::DELEGATION ||
            ns->type() == StateType::LAYOUT)
            recallableStateCount_--;
        if (ns->type() == StateType::LAYOUT)
            clearLayout(lock, ns);
        revokedState_[ns->id()] = ns;
        orderedState_.erase(p);
    }
    state_.clear();
}

void NfsClient::revokeUnreclaimedState()
{
    std::unique_lock<std::mutex> lock(mutex_);
    vector<shared_ptr<NfsState>> toRevoke;
    for (auto& e: state_) {
        auto ns = *e.second;
        if (ns->restored())
            toRevoke.push_back(ns);
    }
    lock.unlock();
    for (auto ns: toRevoke) {
        revokeState(ns);
        clearState(ns->id());
    }
}

void NfsClient::revokeAndClear(std::shared_ptr<NfsState> ns)
{
    if (ns->revoked())
        return;

    if (db_ && db_->isMaster()) {
        auto trans = db_->beginTransaction();
        ns->remove(trans.get());
        db_->commit(move(trans));
    }

    std::unique_lock<std::mutex> lock(mutex_);
    auto fs = ns->fs();
    if (fs) {
        fs->revoke(ns);
    }
    if (ns->type() == StateType::DELEGATION ||
        ns->type() == StateType::LAYOUT)
        recallableStateCount_--;
    if (ns->type() == StateType::LAYOUT)
        clearLayout(lock, ns);

    auto it = state_.find(ns->id());
    assert(it != state_.end());
    auto p = it->second;
    state_.erase(it);
    orderedState_.erase(p);
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
            try {
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
            catch (nfsstat4 stat) {
                LOG(INFO) << "notification failed with status: " << stat;
            }
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

void NfsClient::updateExpiry(
    std::shared_ptr<NfsState> ns,
    util::Clock::time_point oldExpiry,
    util::Clock::time_point newExpiry)
{
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = state_.find(ns->id());
    if (it != state_.end()) {
        auto p = it->second;
        orderedState_.splice(orderedState_.end(), orderedState_, p);
    }
}

void NfsClient::expireState(util::Clock::time_point now)
{
    // Recall any old state which isn't open on this client. The orderedState_
    // list should be roughly sorted by expiry which allows us to
    // avoid examining the entire list.
    //LOG(INFO) << "start expiring state for " << id_;
    std::unique_lock<std::mutex> lock(mutex_);
    std::vector<std::shared_ptr<NfsState>> toRecall;
    for (auto& ns: orderedState_) {
        if (ns->type() == StateType::DELEGATION ||
            ns->type() == StateType::LAYOUT) {
            if (ns->expiry() >= now)
                break;
            auto fs = ns->fs();
            if (fs && fs->isOpen(shared_from_this()))
                continue;
            toRecall.push_back(ns);
            // Rate-limit recalls
            if (toRecall.size() >= 100)
                break;
        }
    }
    lock.unlock();
    for (auto ns: toRecall)
        ns->recall();
    //LOG(INFO) << "done expiring state for " << id_;
}

void NfsClient::reportRevoked()
{
    if (VLOG_IS_ON(1)) {
        std::unique_lock<std::mutex> lock(mutex_);
        int o = 0, d = 0, l = 0;
        for (auto& e: revokedState_) {
            auto ns = e.second;
            switch (ns->type()) {
            case StateType::OPEN:
                o++;
                break;
            case StateType::DELEGATION:
                o++;
                break;
            case StateType::LAYOUT:
                o++;
                break;
            }
        }
        LOG(INFO) << "client " << hex << id_ << dec
                  << " has revoked state"
                  << ": " << o << " opens"
                  << ", " << d << " delegations"
                  << ", " << l << " layouts";
    }
}
