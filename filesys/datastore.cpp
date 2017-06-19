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
#include <sstream>

#include <filesys/filesys.h>
#include <gflags/gflags.h>
#include <rpc++/sockman.h>
#include <rpc++/urlparser.h>

#include "filesys/distfs/distfsproto.h"

using namespace filesys;
using namespace std::literals;

DEFINE_int32(heartbeat, distfs::DISTFS_HEARTBEAT,
             "Number of seconds between hearbeats");

static void reportStatusHelper(
    distfs::DeviceStatus device,
    DataStore* ds,
    std::weak_ptr<oncrpc::SocketManager> sockman,
    std::vector<std::string> addrs)
{
    // Report status now, then schedule a repeat call
    Credential cred{0, 0, {}, true};
    auto fsattr = ds->root()->fsstat(cred);
    distfs::STATUSargs args;
    args.device = device;
    args.storage.totalSpace = fsattr->totalSpace();
    args.storage.freeSpace = fsattr->freeSpace();
    args.storage.availSpace = fsattr->availSpace();

    for (std::string addr: addrs) {
        for (auto& ai: oncrpc::getAddressInfo(addr, "udp")) {
            auto chan = oncrpc::Channel::open(ai);
            distfs::DistfsMds1<oncrpc::SysClient> mds(chan);
            mds.status(args);
        }
    }

    auto t = sockman.lock();
    if (t) {
        auto now = std::chrono::system_clock::now();
        t->add(now + std::chrono::seconds(FLAGS_heartbeat), [=]() {
                reportStatusHelper(device, ds, sockman, addrs);
            });
    }
}

void DataStore::reportStatus(
    std::weak_ptr<oncrpc::SocketManager> sockman, const std::string& url,
    const std::vector<oncrpc::AddressInfo>& addrs,
    const std::vector<oncrpc::AddressInfo>& adminAddrs)
{
    std::random_device rnd;
    distfs::DeviceStatus device;
    std::vector<std::string> mds;

    oncrpc::UrlParser p(url);
    auto mdsRange = p.query.equal_range("mds");
    for (auto it = mdsRange.first; it != mdsRange.second; ++it)
        mds.push_back(it->second);

    for (int i = 0; i < sizeof(device.owner.do_verifier); i++)
        device.owner.do_verifier[i] = rnd();
    device.owner.do_ownerid = fsid();
    for (auto& ai: addrs)
        device.uaddrs.push_back(ai.uaddr());
    for (auto& ai: adminAddrs)
        device.adminUaddrs.push_back(ai.uaddr());

    reportStatusHelper(device, this, sockman, mds);
}
