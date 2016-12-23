/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <random>

#include <rpc++/urlparser.h>

#include <glog/logging.h>
#include <glog/stl_logging.h>
#include "distfs.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace filesys::distfs;
using namespace keyval;
using namespace std;
using namespace std::chrono;
using namespace std::literals;

DistFilesystem::DistFilesystem(
    shared_ptr<keyval::Database> db,
    const vector<string>& addrs,
    shared_ptr<util::Clock> clock)
    : ObjFilesystem(move(db), clock, pieceSize()),
      replicas_(3),
      sockman_(make_shared<oncrpc::SocketManager>()),
      svcreg_(make_shared<oncrpc::ServiceRegistry>())
{
    // Build a clientowner string to use for connecting to devices
    char hostname[256];
    if (::gethostname(hostname, sizeof(hostname)) < 0)
        throw system_error(errno, system_category());
    ostringstream ss;
    ss << "mds" << ::getpgrp() << "@" << hostname;
    clientowner_ = ss.str();

    // Load the entire devices table into memory - this is reasonable
    // since there are likely on the order of 1e3 - 1e4 devices
    devicesNS_ = db_->getNamespace("devices");
    for (auto iterator = devicesNS_->iterator();
         iterator->valid();
         iterator->next()) {
        KeyType k(iterator->key());
        auto id = k.id();
        DeviceStatus val;
        auto buf = iterator->value();
        oncrpc::XdrMemory xm(buf->data(), buf->size());
        xdr(val, static_cast<oncrpc::XdrSource*>(&xm));

        if (id >= nextDeviceId_)
            nextDeviceId_ = id + 1;

        LOG(INFO) << "Restoring device " << id
                  << ": owner " << val.owner;
        auto dev = make_shared<DistDevice>(id, val);
        devicesByOwnerId_[val.owner.do_ownerid] = dev;
        devicesById_[id] = dev;
        devices_.insert(dev);
    }

    // Use the piece table to deduce the next piece index for each
    // device entry
    piecesNS_ = db_->getNamespace("pieces");
    for (auto& dev: devices_) {
        auto iter = piecesNS_->iterator(DoubleKeyType(dev->id(), ~0ull));
        if (iter->valid()) {
            iter->prev();
        }
        else {
            iter->seekToLast();
        }
        if (!iter->valid()) {
            dev->setNextPieceIndex(0);
        }
        else {
            DoubleKeyType k(iter->key());
            if (k.id0() == dev->id())
                dev->setNextPieceIndex(k.id1() + 1);
            else
                dev->setNextPieceIndex(0);
        }
        VLOG(1) << "Device " << dev->id()
                << ": next piece index " << dev->nextPieceIndex();
    }

    // Schedule resilvering for anything still in the repairs table.
    // We delay any repair attempts for 2*DISTFS_HEARTBEAT to allow time
    // to reconnect with the data servers
    repairsNS_ = db_->getNamespace("repairs");
    if (db_->isMaster()) {
        auto delay = chrono::milliseconds(2000*DISTFS_HEARTBEAT);
        for (auto iterator = repairsNS_->iterator();
             iterator->valid();
             iterator->next()) {
            PieceData key(iterator->key());
            PieceId id{key.fileid(), key.offset(), key.size()};
            resilverPiece(id, delay);

            // Rate limit resilvering to 100 pieces/sec
            delay += 10ms;
        }
    }

    bind(svcreg_);
    if (addrs.size() > 0) {
        // Bind to the device discovery port so that we can receive status
        // updates from the device fleet.
        //
        // For replicated metadata servers, the given addr may have
        // several addresses, some of which are not for this
        // server. Just ignore any bind errors but make sure we bind
        // to at least one address.
        int bound = false;
        exception_ptr lastError;
        for (auto& addr: addrs) {
            for (auto& ai: oncrpc::getAddressInfo(addr)) {
                try {
                    int fd = socket(ai.family, ai.socktype, ai.protocol);
                    if (fd < 0)
                        throw system_error(errno, system_category());
                    int one = 1;
                    ::setsockopt(
                        fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
                    ::setsockopt(
                        fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
                    auto sock = make_shared<oncrpc::DatagramChannel>(
                        fd, svcreg_);
                    sock->bind(ai.addr);
                    sockman_->add(sock);
                    bound = true;
                }
                catch (system_error& e) {
                    lastError = std::current_exception();
                }
            }
        }
        if (!bound) {
            std::rethrow_exception(lastError);
        }
    }

    thread_ = thread(
        [this]() {
            sockman_->run();
        });
}

DistFilesystem::DistFilesystem(
    shared_ptr<Database> db, const vector<string>& addrs)
    : DistFilesystem(move(db), addrs, make_shared<util::SystemClock>())
{
}

DistFilesystem::~DistFilesystem()
{
    unique_lock<mutex> lk(mutex_);
    stopping_ = true;
    lk.unlock();

    sockman_->stop();
    thread_.join();
    unbind(svcreg_);
    dscache_.clear();
}

vector<shared_ptr<Device>> DistFilesystem::devices(uint64_t& gen)
{
    vector<shared_ptr<Device>> res;
    for (auto& entry: devicesById_) {
        res.push_back(entry.second);
    }
    return res;
}

shared_ptr<Device> DistFilesystem::findDevice(std::uint64_t& devid)
{
    auto it = devicesById_.find(devid);
    if (it == devicesById_.end())
        throw system_error(ENOENT, system_category());
    return it->second;
}

shared_ptr<ObjFile> DistFilesystem::makeNewFile(FileId fileid)
{
    return make_shared<DistFile>(shared_from_this(), fileid);
}

shared_ptr<ObjFile> DistFilesystem::makeNewFile(ObjFileMetaImpl&& meta)
{
    return make_shared<DistFile>(shared_from_this(), move(meta));
}

shared_ptr<OpenFile> DistFilesystem::makeNewOpenFile(
    const Credential& cred, shared_ptr<ObjFile> file, int flags)
{
    return make_shared<DistOpenFile>(
        cred, dynamic_pointer_cast<DistFile>(file), flags);
}

void
DistFilesystem::databaseMasterChanged(bool isMaster)
{
    ObjFilesystem::databaseMasterChanged(isMaster);
}

void DistFilesystem::status(const STATUSargs& args)
{
    VLOG(2) << "Received status for device: " << args.device.owner;

    unique_lock<mutex> lk(mutex_);
    if (stopping_)
        return;

    auto it = devicesByOwnerId_.find(args.device.owner.do_ownerid);
    shared_ptr<DistDevice> dev;
    bool needWrite = false;
    if (it == devicesByOwnerId_.end()) {
        int id = nextDeviceId_++;
        LOG(INFO) << "Adding new device: " << args.device.owner
                  << ", id: " << id;
        dev = make_shared<DistDevice>(id, args.device, args.storage);
        dev->calculatePriority();
        if (db_->isMaster()) {
            // If we are the master replica, schedule a task to verify
            // the device's piece collection
            dev->setState(DistDevice::RESTORING);
        }
        devicesByOwnerId_[args.device.owner.do_ownerid] = dev;
        devicesById_[id] = dev;
        devices_.insert(dev);
        devicesGen_++;
        needWrite = true;
    }
    else {
        // Update our entry for this device and re-sort its position
        // in the devices_ list which is ordered by capacity
        dev = it->second;
        storage_.totalSpace -= dev->storage().totalSpace;
        storage_.freeSpace -= dev->storage().freeSpace;
        storage_.availSpace -= dev->storage().availSpace;
        devices_.erase(dev);
        needWrite = dev->update(
            shared_from_this(),sockman_, args.device, args.storage);
        dev->calculatePriority();
        devices_.insert(dev);
    }
    storage_.totalSpace += args.storage.totalSpace;
    storage_.freeSpace += args.storage.freeSpace;
    storage_.availSpace += args.storage.availSpace;
    if (db_->isMaster() && dev->state() == DistDevice::RESTORING) {
        // Defer writing the device entry until we are done
        // restoring it
        auto it = devicesToRestore_.find(dev);
        if (it == devicesToRestore_.end()) {
            // Purge any existing connection to the device since it
            // has apparently restarted
            dscache_.remove(dev->id());
            LOG(INFO) << "Scheduling a task to restore device " << dev->id();
            devicesToRestore_.insert(dev);
            sockman_->add(
                system_clock::now(),
                [dev, this]() {
                    restoreDevice(dev);
                });
        }
        needWrite = false;
    }
    lk.unlock();
    if (needWrite && db_->isMaster()) {
        dev->write(shared_from_this());
    }
}

shared_ptr<DistPiece>
DistFilesystem::findPiece(PieceId id, bool create, Transaction* trans)
{
    return piececache_.find(
        id,
        [](auto) {},
        [this, create, trans](const PieceId& id) {
            try {
                auto buf = dataNS_->get(PieceData(id));
                oncrpc::XdrMemory xm(buf->data(), buf->size());
                PieceLocation loc;
                xdr(loc, static_cast<oncrpc::XdrSource*>(&xm));
                return make_shared<DistPiece>(
                    dynamic_pointer_cast<DistFilesystem>(shared_from_this()),
                    id, loc);
            }
            catch (system_error& e) {
                if (e.code().value() != ENOENT || !create)
                    throw;

                VLOG(1) << "Creating new piece " << id;

                auto res = make_shared<DistPiece>(
                    dynamic_pointer_cast<DistFilesystem>(shared_from_this()),
                    id);
                addPieceLocations(res, replicas_, false, trans);

                return res;
            }
        });
}

void
DistFilesystem::removePiece(
    const Credential& cred, PieceId id, Transaction* trans)
{
    VLOG(1) << "Removing piece " << id;
    auto piece = findPiece(id, false, nullptr);
    piece->remove(cred, trans);
    piececache_.remove(id);
}

void
DistFilesystem::resilverPiece(PieceId id, system_clock::duration delay)
{
    LOG(INFO) << "Resilvering " << id << " after "
              << duration_cast<milliseconds>(delay).count() << " ms";
    repairQueueSize_++;
    sockman_->add(
        system_clock::now() + delay,
        [this, id]() {
            assert(db_->isMaster());
            repairQueueSize_--;
            auto piece = findPiece(id, false, nullptr);
            int n = replicas_ - int(piece->loc().size());
            if (n > 0) {
                auto trans = db_->beginTransaction();
                try {
                    addPieceLocations(piece, n, true, trans.get());
                }
                catch (system_error&) {
                    resilverPiece(id, 30s);
                    return;
                }
                db_->commit(move(trans));
            }
        });
}

void
DistFilesystem::addPieceLocations(
    shared_ptr<DistPiece> piece, int count, bool resilver, Transaction* trans)
{
    Credential cred(0, 0, {}, true);
    auto id = piece->id();

    VLOG(1) << "Creating locations for piece " << id;
    unordered_set<devid> existingDevices;
    for (auto loc: piece->loc()) {
        VLOG(1) << "Existing location device " << loc.device;
        existingDevices.insert(loc.device);
    }

    // Take the set of devices with the most capacity
    // to use for the new piece
    unique_lock<mutex> lk(mutex_);
    PieceLocation loc;
    vector<shared_ptr<DistDevice>> replicas;
    vector<shared_ptr<File>> files;

    VLOG(1) << devices_.size() << " active devices";
    for (int i = 0; i < count; i++) {
    retry:
        assert(devices_.size() > 0);
        auto it = devices_.rbegin();
        auto dev = *it;
        for (;;) {
            // Don't choose a device which already has a copy of the
            // piece or which has a zero priority
            VLOG(1) << "Trying device " << dev->id()
                    << ": priority " << dev->priority();
            if (dev->priority() == 0 ||
                existingDevices.find(dev->id()) != existingDevices.end()) {
                ++it;
                if (it == devices_.rend()) {
                    LOG(ERROR) << "No viable locations for piece";

                    // If we created any pieces, try to reverse the
                    // action. If we fail any of the removals, we can
                    // deal with it later in restoreDevice when that
                    // device rejoins
                    for (auto dev: replicas) {
                        try {
                            auto ds = findDataStore(dev->id());
                            ds->removePiece(cred, id);
                        }
                        catch (system_error&) {
                        }
                        devices_.insert(dev);
                    }
                    throw system_error(EIO, system_category());
                }
                dev = *it;
                continue;
            }
            break;
        }
        devices_.erase(dev);
        assert(existingDevices.find(dev->id()) == existingDevices.end());
        lk.unlock();
        shared_ptr<DataStore> ds;
        try {
            ds = findDataStore(dev->id());
            files.push_back(ds->createPiece(cred, id));
        }
        catch (system_error& e) {
            LOG(INFO) << "Device " << dev->id() << ": failed to create piece "
                      << id;

            // Set the device priority to zero and retry
            lk.lock();
            dev->setPriority(0);
            devices_.insert(dev);
            goto retry;
        }
        lk.lock();
        VLOG(1) << "Device " << dev->id() << ": piece created";
        replicas.push_back(dev);
        loc.push_back(
            PieceIndex{devid(dev->id()), dev->newPieceIndex()});
        existingDevices.insert(dev->id());
    }
    lk.unlock();

    // Record our choices in the pieces table
    for (int i = 0; i < count; i++) {
        auto& entry = loc[i];
        DoubleKeyType pieceKey(entry.device, entry.index);
        PieceData pieceData(id);
        trans->put(piecesNS_, pieceKey, pieceData);
    }

    // Add the new locations and resilver if required. This will also record
    // the locations in the data table
    piece->addPieceLocations(loc, move(files), resilver, trans);

    // Put back the replicas we used into the device list
    lk.lock();
    for (auto dev: replicas)
        devices_.insert(dev);
    lk.unlock();
}

shared_ptr<DataStore>
DistFilesystem::findDataStore(devid id)
{
    return dscache_.find(
        id,
        [](auto) {},
        [this](auto id) {
            auto it = devicesById_.find(id);
            if (it == devicesById_.end())
                throw system_error(ENOENT, system_category());
            auto dev = it->second;
            for (auto& ai: dev->addrs()) {
                try {
                    LOG(INFO) << "Device " << id << ": connecting to "
                              << ai.uaddr();
                    auto chan = oncrpc::Channel::open(ai);
                    auto client = std::make_shared<oncrpc::SysClient>(
                        nfs4::NFS4_PROGRAM, nfs4::NFS_V4);
                    return make_shared<nfs4::NfsDataStore>(
                        chan, client, clock_, clientowner_);
                }
                catch (system_error& e) {
                    continue;
                }
            }
            LOG(ERROR) << "Can't connect to device " << dev->id();
            throw system_error(ENOTCONN, system_category());
        });
}

void DistFilesystem::addDataStore(std::shared_ptr<DataStore> ds)
{
    auto fsid = ds->fsid();
    distfs_owner owner;
    owner.do_verifier = {{0,0,0,0, 0,0,0,0}};
    owner.do_ownerid = fsid;

    int id = nextDeviceId_++;
    auto dev = make_shared<DistDevice>(
        id, DeviceStatus{owner, {}}, StorageStatus{0, 0, 0});
    dev->setState(DistDevice::HEALTHY);
    devicesByOwnerId_[owner.do_ownerid] = dev;
    devicesById_[id] = dev;
    devices_.insert(dev);
    devicesGen_++;
    dscache_.add(dev->id(), ds);
}

void DistFilesystem::restoreDevice(std::shared_ptr<DistDevice> dev)
{
    LOG(INFO) << "Restoring device " << dev->id();

    assert(db_->isMaster());

    shared_ptr<DataStore> ds;
    try {
        ds = findDataStore(dev->id());
    }
    catch (system_error& e) {
        unique_lock<mutex> lk(mutex_);
        dev->setState(DistDevice::UNKNOWN);
        dev->scheduleTimeout(shared_from_this(), sockman_);
        devicesToRestore_.erase(dev);
        return;
    }

    // Use the pieces namespace to build a list of the pieces that we
    // believe the device should have
    map<PieceId, uint64_t> expectedPieces;
    DoubleKeyType sk(dev->id(), 0);
    DoubleKeyType ek(dev->id(), ~0ull);
    for (auto iter = piecesNS_->iterator(sk); iter->valid(ek); iter->next()) {
        auto val = PieceData(iter->value());
        auto id = PieceId{val.fileid(), val.offset(), val.size()};
        VLOG(2) << "Expected piece " << id;
        expectedPieces[id] = DoubleKeyType(iter->key()).id1();
    }

    // Iterate over the device's piece collection and check that it
    // matches what we expect to see
    Credential cred(0, 0, {}, true);
    vector<PieceId> toAdd;
    vector<PieceId> toRemove;
    try {
        for (auto iter = ds->root()->readdir(cred, 0);
             iter->valid(); iter->next()) {
            // Names are formatted as '<fileid>-<log2size>-<index>' with
            // <fileid> in hex, log2size and index in decimal
            if (iter->name() == "." || iter->name() == "..")
                continue;
            istringstream ss(iter->name());
            uint32_t l2size;
            uint64_t fileid, index;
            ss >> hex >> fileid >> dec;
            ss.get();
            ss >> l2size;
            ss.get();
            ss >> index;
            auto pieceId = l2size == 64 ?
                PieceId{FileId(fileid), 0, 0} :
            PieceId{FileId(fileid), index << l2size, 1u << l2size};
            VLOG(2) << "Checking piece " << pieceId;
            try {
                auto piece = findPiece(pieceId, false, nullptr);
                if (!piece->hasLocation(dev->id())) {
                    // The piece entry doesn't list this device - purge the
                    // device's copy of the piece
                    LOG(INFO) << "Device " << dev->id()
                              << ": unexpected copy of piece " << pieceId
                              << ": removing";
                    toRemove.push_back(pieceId);
                }
                else if (expectedPieces.find(pieceId) == expectedPieces.end()) {
                    // This seems unlikely to happen - the pieces
                    // namespace is out of sync with the data
                    // namespace. We can fixed it easily enough though.
                    LOG(INFO) << "Device " << dev->id()
                              << ": valid copy of piece " << pieceId
                              << " which is not in the pieces table: repairing";
                    toAdd.push_back(pieceId);
                }
            }
            catch (system_error& e) {
                // The listed piece doesn't exist in our database
                LOG(INFO) << "Device " << dev->id()
                          << ": unknown piece " << pieceId
                          << ": removing";
                toRemove.push_back(pieceId);
            }
            expectedPieces.erase(pieceId);
        }
    }
    catch (system_error& e) {
        unique_lock<mutex> lk(mutex_);
        dev->setState(DistDevice::UNKNOWN);
        dev->scheduleTimeout(shared_from_this(), sockman_);
        devicesToRestore_.erase(dev);
        return;
    }

    if (expectedPieces.size() > 0) {
        auto trans = db_->beginTransaction();
        auto delay = 0ms;
        for (auto& entry: expectedPieces) {
            auto& id = entry.first;
            try {
                auto piece = findPiece(id, false, nullptr);
                if (!piece->hasLocation(dev->id()))
                    throw system_error(ENOENT, system_category());
                LOG(INFO) << "Device " << dev->id()
                          << ": missing piece " << id;
                piece->removeBadLocation(dev->id(), delay, trans.get());

                // Rate limit resilvering to 100 pieces/sec
                delay += 10ms;
            }
            catch (system_error&) {
                LOG(ERROR) << "Pieces list contains stale entry " << id
                           << ": removing";
                trans->remove(
                    piecesNS_, DoubleKeyType(dev->id(), entry.second));
            }
        }
        db_->commit(move(trans));
    }

    for (auto& id: toRemove) {
        try {
            ds->removePiece(cred, id);
        }
        catch (system_error&) {
            LOG(ERROR) << "Failed to remove " << id << ": ignoring";
        }
    }

    unique_lock<mutex> lk(mutex_);

    if (toAdd.size() > 0) {
        auto trans = db()->beginTransaction();
        for (auto& id: toAdd) {
            trans->put(
                piecesNS_,
                DoubleKeyType(dev->id(), dev->newPieceIndex()),
                PieceData(id));
        }
        db()->commit(move(trans));
    }

    LOG(INFO) << "Device " << dev->id() << ": piece collection validated";
    dev->setState(DistDevice::HEALTHY);
    dev->scheduleTimeout(shared_from_this(), sockman_);
    dev->setPriority(1);
    dev->write(shared_from_this());
    devicesToRestore_.erase(dev);
}

void
DistFilesystem::decommissionDevice(std::shared_ptr<DistDevice> dev)
{
    if (!db_->isMaster())
        return;

    // Stop accounting for this device in our storage summary
    storage_.totalSpace -= dev->storage().totalSpace;
    storage_.freeSpace -= dev->storage().freeSpace;
    storage_.availSpace -= dev->storage().availSpace;
    dev->clearStorage();

    // We want to avoid mass decommissioning in the face of a network
    // partition. Try to detect this by figuring out what fraction of
    // the fleet is working (which we define as either HEALTHY or
    // RESTORING).
    int liveCount = 0;
    for (auto p: devices_) {
        if (p->state() == DistDevice::RESTORING ||
            p->state() == DistDevice::HEALTHY)
            liveCount++;
    }

    if (liveCount <= devices_.size() / 2) {
        // If 50% or more of the fleet is missing, don't decommission
        // this device - set its state back to MISSING
        LOG(INFO) << "Too many devices are missing"
                  << ": not decommisioning device " << dev->id();
        dev->setState(DistDevice::MISSING);
        dev->scheduleTimeout(shared_from_this(), sockman_);
    }

    LOG(INFO) << "Device " << dev->id() << ": decommisioning";

    // Get a list of the pieces which this device has and resilver them
    map<PieceId, uint64_t> expectedPieces;
    DoubleKeyType sk(dev->id(), 0);
    DoubleKeyType ek(dev->id(), ~0ull);
    for (auto iter = piecesNS_->iterator(sk); iter->valid(ek); iter->next()) {
        auto val = PieceData(iter->value());
        auto id = PieceId{val.fileid(), val.offset(), val.size()};
        expectedPieces[id] = DoubleKeyType(iter->key()).id1();
    }

    auto trans = db_->beginTransaction();

    auto delay = 0ms;
    for (auto& entry: expectedPieces) {
        auto& id = entry.first;
        try {
            auto piece = findPiece(id, false, nullptr);
            piece->removeBadLocation(dev->id(), delay, trans.get());

            // Rate limit resilvering to 100 pieces/sec
            delay += 10ms;
        }
        catch (system_error&) {
            LOG(ERROR) << "Pieces list contains stale entry " << id
                       << ": removing";
            trans->remove(piecesNS_, DoubleKeyType(dev->id(), entry.second));
        }
    }

    unique_lock<mutex> lk(mutex_);
    devicesByOwnerId_.erase(dev->owner().do_ownerid);
    devicesById_.erase(dev->id());
    devices_.erase(dev);
    devicesGen_++;

    trans->remove(devicesNS_, KeyType(dev->id()));
    db_->commit(move(trans));
}

shared_ptr<Filesystem>
DistFilesystemFactory::mount(const string& url)
{
    LOG(INFO) << "mount: " << url;
    oncrpc::UrlParser p(url);
    vector<string> addrs;
    vector<string> replicas;

    auto mdsRange = p.query.equal_range("mds");
    for (auto it = mdsRange.first; it != mdsRange.second; ++it)
        addrs.push_back(it->second);

    auto replicaRange = p.query.equal_range("replica");
    for (auto it = replicaRange.first; it != replicaRange.second; ++it)
        replicas.push_back(it->second);

    shared_ptr<Database> db;
    if (replicas.size() > 0) {
        db = make_paxosdb(p.path, replicas);
    }
    else {
        db = make_rocksdb(p.path);
    }
    return make_shared<DistFilesystem>(db, addrs);
};

void filesys::distfs::init(FilesystemManager* fsman)
{
    oncrpc::UrlParser::addPathbasedScheme("distfs");
    fsman->add(make_shared<DistFilesystemFactory>());
}
