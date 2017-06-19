/*-
 * Copyright (c) 2016-present Doug Rabson
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <random>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "distfs.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace filesys::distfs;
using namespace keyval;
using namespace std;
using namespace std::chrono;

DistDevice::DistDevice(
    int id, const DeviceStatus& status)
    : id_(id),
      owner_(status.owner),
      uaddrs_(status.uaddrs),
      storage_({0, 0, 0}),
      priority_(1.0f),
      nextPieceIndex_(1),
      state_(UNKNOWN)
{
    // Don't bother to resolve the addresses until we actually get an
    // update from the device
}

DistDevice::DistDevice(
    int id, const DeviceStatus& status, const StorageStatus& storage)
    : id_(id),
      owner_(status.owner),
      uaddrs_(status.uaddrs),
      storage_(storage),
      priority_(1.0f),
      nextPieceIndex_(1),
      state_(UNKNOWN)
{
    resolveAddresses();
}

vector<oncrpc::AddressInfo> DistDevice::addresses() const
{
    auto lk = lock();
    return addrs_;
}

vector<oncrpc::AddressInfo> DistDevice::adminAddresses() const
{
    auto lk = lock();
    return adminAddrs_;
}

Device::CallbackHandle DistDevice::addStateCallback(
    std::function<void(State)> cb)
{
    auto lk = lock();
    auto h = nextCbHandle_++;
    callbacks_[h] = cb;
    return h;
}

void DistDevice::removeStateCallback(CallbackHandle h)
{
    auto lk = lock();
    callbacks_.erase(h);
}

bool DistDevice::update(
    weak_ptr<DistFilesystem> fs,
    shared_ptr<oncrpc::TimeoutManager> tman,
    const DeviceStatus& status, const StorageStatus& storage)
{
    auto lk = lock();
    bool res = false;
    bool addressChanged = false;
    bool needResolve = false;
    if (owner_ != status.owner) {
        LOG(INFO) << "Device " << id_
                  << ": owner changed: "
                  << owner_ << " -> " << status.owner;
        owner_ = status.owner;

        // The device has restarted, we need to verify its piece
        // collection before we can use it safely
        setState(lk, RESTORING);
        res = true;
    }
    else if (state_ == MISSING || state_ == DEAD) {
        // Device has returned to the fleet after we have previously
        // marked it as defective. We need to re-validate it before
        // its ready for use.
        setState(lk, RESTORING);
    }
    else if (state_ == UNKNOWN) {
        // This happens during startup as we re-discover devices
        // which were marked UNKNOWN when we read then from the
        // database.
        setState(lk, HEALTHY);
    }
    if (uaddrs_ != status.uaddrs) {
        LOG(INFO) << "Device " << id_
                  << ": uaddrs changed: "
                  << uaddrs_ << " -> " << status.uaddrs;
        uaddrs_ = status.uaddrs;
        res = true;
        addressChanged = true;
        needResolve = true;
    }
    else if (addrs_.size() == 0) {
        needResolve = true;
    }
    if (adminUaddrs_ != status.adminUaddrs) {
        LOG(INFO) << "Device " << id_
                  << ": adminUaddrs changed: "
                  << adminUaddrs_ << " -> " << status.adminUaddrs;
        adminUaddrs_ = status.adminUaddrs;
        res = true;
        addressChanged = true;
        needResolve = true;
    }
    else if (adminAddrs_.size() == 0) {
        needResolve = true;
    }
    if (needResolve) {
        resolveAddresses();
}
    storage_ = storage;
    if (addressChanged) {
        auto cbs = callbacks_;
        lk.unlock();
        for (auto& entry: cbs)
            entry.second(ADDRESS_CHANGED);
    }
    else {
        lk.unlock();
    }

    scheduleTimeout(fs, tman);

    return res;
}

void DistDevice::calculatePriority()
{
    auto lk = lock();
    if (storage_.totalSpace) {
        priority_ = float(storage_.availSpace) / float(storage_.totalSpace);
    }
    else {
        priority_ = 0.0f;
    }
    //LOG(INFO) << "Device " << id_ << ": priority " << priority_;
}

void DistDevice::setState(unique_lock<mutex>& lk, State state)
{
    static const char* stateNames[] = {
        "UNKNOWN", "RESTORING", "MISSING", "DEAD", "HEALTHY"
    };

    LOG(INFO) << "Device " << id_
              << ": setting state to " << stateNames[state];
    state_ = state;
    auto cbs = callbacks_;

    lk.unlock();
    for (auto& entry: cbs)
        entry.second(state);
    lk.lock();
}

void DistDevice::write(shared_ptr<DistFilesystem> fs)
{
    LOG(INFO) << "Device " << id_ << ": writing to database";

    assert(fs->db()->isMaster());

    DeviceStatus val;
    val.owner = owner_;
    val.uaddrs = uaddrs_;
    auto buf = make_shared<Buffer>(oncrpc::XdrSizeof(val));
    oncrpc::XdrMemory xm(buf->data(), buf->size());
    xdr(val, static_cast<oncrpc::XdrSink*>(&xm));

    auto trans = fs->db()->beginTransaction();
    trans->put(fs->devicesNS(), KeyType(id_), buf);
    trans->put(fs->piecesNS(), DoubleKeyType(id_, 0), KeyType(nextPieceIndex_));
    fs->db()->commit(move(trans));
}

void DistDevice::scheduleTimeout(
    weak_ptr<DistFilesystem> fs,
    weak_ptr<oncrpc::TimeoutManager> tman)
{
    auto p = tman.lock();
    if (!p)
        return;

    static random_device dev;
    static default_random_engine rnd(dev());
    int heartbeat_ms = 1000*DISTFS_HEARTBEAT;
    uniform_int_distribution<> dist(
        -heartbeat_ms / 8, heartbeat_ms / 8);

    if (timeout_) {
        p->cancel(timeout_);
        timeout_ = 0;
    }

    auto now = system_clock::now();
    switch (state_) {
    case RESTORING:
    case DEAD:
    case ADDRESS_CHANGED:
        break;

    case MISSING:
        timeout_ = p->add(
            now + milliseconds(8*heartbeat_ms + dist(rnd)),
            [this, fs]() {
                setState(DEAD);
                fs.lock()->decommissionDevice(shared_from_this());
            });
        break;

    case UNKNOWN:
    case HEALTHY:
        timeout_ = p->add(
            now + milliseconds(2*heartbeat_ms + dist(rnd)),
            [this, fs, tman]() {
                setState(MISSING);
                scheduleTimeout(fs, tman);
            });
        break;
    }
}

void DistDevice::resolveAddresses()
{
    // If any of the addresses given are wildcard addresses,
    // substitute the RPC channel's remote address, using the
    // requested port rather than the RPC source port
    addrs_.clear();
    for (auto& uaddr: uaddrs_) {
        auto ai = oncrpc::AddressInfo::fromUaddr(uaddr, "tcp");
        if (ai.isWildcard()) {
            auto port = ai.port();
            auto chan = oncrpc::CallContext::current().channel();
            auto chanAddr = chan->remoteAddress();
            if (ai.family == chanAddr.family) {
                ai.addr = chanAddr.addr;
                ai.setPort(port);
            }
            else {
                continue;
            }
        }
        addrs_.push_back(ai);
    }

    adminAddrs_.clear();
    for (auto& uaddr: adminUaddrs_) {
        auto ai = oncrpc::AddressInfo::fromUaddr(uaddr, "tcp");
        if (ai.isWildcard()) {
            auto port = ai.port();
            auto chan = oncrpc::CallContext::current().channel();
            auto chanAddr = chan->remoteAddress();
            if (ai.family == chanAddr.family) {
                ai.addr = chanAddr.addr;
                ai.setPort(port);
            }
            else {
                continue;
            }
        }
        adminAddrs_.push_back(ai);
    }
}
