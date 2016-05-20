#include <random>

#include <fs++/filesys.h>
#include <gflags/gflags.h>

#include "filesys/distfs/distfsproto.h"

using namespace filesys;
using namespace std::literals;

DEFINE_int32(heartbeat, distfs::DISTFS_HEARTBEAT,
             "Number of seconds between hearbeats");

static void reportStatusHelper(
    distfs::DeviceStatus device,
    DataStore* ds,
    std::weak_ptr<oncrpc::SocketManager> sockman,
    std::string addr)
{
    // Report status now, then schedule a repeat call
    Credential cred{0, 0, {}, true};
    auto fsattr = ds->root()->fsstat(cred);
    distfs::STATUSargs args;
    args.device = device;
    args.storage.totalSpace = fsattr->tbytes();
    args.storage.freeSpace = fsattr->fbytes();
    args.storage.availSpace = fsattr->abytes();
    auto chan = oncrpc::Channel::open(addr);
    distfs::DistfsMds1<oncrpc::SysClient> mds(chan);
    mds.status(args);

    auto t = sockman.lock();
    if (t) {
        auto now = std::chrono::system_clock::now();
        t->add(now + std::chrono::seconds(FLAGS_heartbeat), [=]() {
                reportStatusHelper(device, ds, sockman, addr);
            });
    }
}

void DataStore::reportStatus(
    std::weak_ptr<oncrpc::SocketManager> sockman, const std::string& addr,
    const std::vector<oncrpc::AddressInfo>& boundAddrs)
{
    std::random_device rnd;
    distfs::DeviceStatus device;

    for (int i = 0; i < sizeof(device.owner.do_verifier); i++)
        device.owner.do_verifier[i] = rnd();
    device.owner.do_ownerid = fsid();
    for (auto& ai: boundAddrs)
        device.uaddrs.push_back(ai.uaddr());

    reportStatusHelper(device, this, sockman, addr);
}

