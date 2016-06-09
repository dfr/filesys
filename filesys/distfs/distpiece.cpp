/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include "distfs.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace filesys::distfs;
using namespace keyval;
using namespace std;
using namespace std::chrono;

PieceId DistPiece::id() const
{
    return id_;
}

int DistPiece::mirrorCount() const
{
    return loc_.size();
}

std::pair<std::shared_ptr<Device>, std::shared_ptr<File>>
DistPiece::mirror(const Credential& cred, int i)
{
    auto fs = fs_.lock();
    auto devid = loc_[i].device;
    auto dev = fs->lookupDevice(devid);
    if (dev->state() == DistDevice::HEALTHY) {
        try {
            auto ds = fs->findDataStore(devid);
            if (!files_[i]) {
                // This is the first time we have tried to use
                // this DS - look up the piece now
                files_[i] = ds->findPiece(cred, id_);
            }
            return make_pair(dev, files_[i]);
        }
        catch (system_error&) {
            LOG(ERROR) << "Device " << devid << ": lookup failed";
            dev->setState(DistDevice::MISSING);
            files_[i].reset();
            of_[i].reset();
        }
    }
    else {
        LOG(ERROR) << "Device " << devid
                   << ": not healthy, removing location";
    }
    auto trans = fs->db()->beginTransaction();
    removeBadLocation(devid, 0s, trans.get());
    fs->db()->commit(move(trans));
    throw system_error(EIO, system_category());
}

void DistPiece::setState(State state)
{
    static const char* stateNames[] = {
        "IDLE", "BUSY", "RECALLING", "NEED_RESILVER", "RESILVERING"
    };
    VLOG(1) << "Piece " << id_
            << ": setting state to " << stateNames[state];
    state_ = state;
}

void DistPiece::addPieceLocations(
    const PieceLocation& loc,
    std::vector<std::shared_ptr<File>>&& files, bool resilver,
    Transaction* trans)
{
    auto lk = lock();
    int n = int(loc_.size());
    if (n == 0) {
        loc_ = loc;
        files_ = std::move(files);
    }
    else {
        loc_.insert(loc_.end(), loc.begin(), loc.end());
        files_.insert(files_.end(), files.begin(), files.end());
    }
    of_.resize(loc_.size());
    if (int(loc_.size()) > targetCopies_)
        targetCopies_ = loc_.size();
    else
        assert(int(loc_.size()) == targetCopies_);

    std::unordered_set<devid> bad;
    if (n > 0 && resilver) {
        setState(RESILVERING);
        for (int i = n; i < int(loc_.size()); i++) {
            if (!resilverLocation(lk, i))
                bad.insert(loc_[i].device);
        }
        setState(IDLE);
    }
    else {
        setState(IDLE);
    }

    if (bad.size() == 0) {
        // Write the new set of locations to the data table
        auto buf = make_shared<Buffer>(oncrpc::XdrSizeof(loc_));
        oncrpc::XdrMemory xm(buf->data(), buf->size());
        xdr(loc_, static_cast<oncrpc::XdrSink*>(&xm));
        PieceData key(id_);
        auto fs = fs_.lock();
        trans->put(fs->dataNS(), key, buf);
        trans->remove(fs->repairsNS(), key);
    }
    else {
        // If any of the copies failed, schedule another resilver
        // attempt later
        removeBadLocations(lk, bad, std::chrono::seconds(30), trans);
    }
}

shared_ptr<Buffer>
DistPiece::read(const Credential& cred, int off, int sz)
{
    if (id_.size > 0) {
        assert(off >= id_.offset && off + sz <= id_.offset + id_.size);
    }

    auto lk = lock();
    auto fs = fs_.lock();
    int count = 0;
    shared_ptr<Buffer> res;


    // As policy, if we fail to read from a device, we don't
    // immediately resilver the piece. This may be a transient failure
    // so we mark the device as missing. If the device stays missing,
    // we will eventually mark it as dead and resilver all its pieces.
    while (count < loc_.size()) {
        auto devid = loc_[index_].device;
        auto dev = fs->lookupDevice(devid);
        try {
            if (dev->state() == DistDevice::HEALTHY) {
                auto ds = fs->findDataStore(devid);
                if (!files_[index_]) {
                    // This is the first time we have tried to use
                    // this DS - look up the piece now
                    files_[index_] = ds->findPiece(cred, id_);
                }
                shared_ptr<OpenFile> of;
                if (!of_[index_]) {
                    auto file = files_[index_];
                    of = file->open(cred, OpenFlags::RDWR);
                    of_[index_] = of;
                }
                else {
                    of = of_[index_];
                }
                bool eof;
                res = of->read(off, sz, eof);
                break;
            }
        }
        catch (system_error&) {
            LOG(ERROR) << "Device " << dev << ": read failed";
            dev->setState(DistDevice::MISSING);
            files_[index_].reset();
            of_[index_].reset();
        }
        count++;
        index_++;
        if (index_ == loc_.size())
            index_ = 0;
    }

    // If we have tried all locations, throw an appropriate error
    if (!res)
        throw system_error(EIO, system_category());

    return res;
}

int DistPiece::write(
    const Credential& cred, int off, shared_ptr<Buffer> buf,
    Transaction* trans)
{
    if (id_.size > 0) {
        assert(off >= id_.offset && off + buf->size() <= id_.offset + id_.size);
    }

    auto lk = lock();
    auto fs = fs_.lock();
    unordered_set<devid> bad;
    for (int i = 0; i < int(loc_.size()); i++) {
        auto devid = loc_[i].device;
        auto dev = fs->lookupDevice(devid);
        if (dev->state() == DistDevice::HEALTHY) {
            try {
                auto ds = fs->findDataStore(devid);
                if (!files_[i]) {
                    files_[i] = ds->findPiece(cred, id_);
                }
                shared_ptr<OpenFile> of;
                if (!of_[i]) {
                    auto file = files_[i];
                    of = file->open(cred, OpenFlags::RDWR);
                    of_[i] = of;
                }
                else {
                    of = of_[i];
                }
                of->write(off, buf); // XXX check result
            }
            catch (system_error&) {
                LOG(ERROR) << "Device " << devid << ": write failed";
                dev->setState(DistDevice::MISSING);
                files_[i].reset();
                of_[i].reset();
                bad.insert(devid);
            }
        }
        else {
            LOG(ERROR) << "Device " << devid
                       << ": not healthy, removing location";
            bad.insert(devid);
        }
    }
    removeBadLocations(lk, bad, 0s, trans);

    return buf->size();
}

void DistPiece::close()
{
    for (auto& of: of_)
        of.reset();
}

void DistPiece::truncate(
    const Credential& cred, int newSize, Transaction* trans)
{
    auto lk = lock();
    auto fs = fs_.lock();
    unordered_set<devid> bad;
    for (int i = 0; i < int(loc_.size()); i++) {
        auto devid = loc_[i].device;
        auto dev = fs->lookupDevice(devid);
        if (dev->state() == DistDevice::HEALTHY) {
            try {
                auto ds = fs->findDataStore(devid);
                if (!files_[i]) {
                    files_[i] = ds->findPiece(cred, id_);
                }
                auto file = files_[i];
                if (newSize != file->getattr()->size()) {
                    file->setattr(
                        cred,
                        [newSize](auto sa) { sa->setSize(newSize); });
                }
            }
            catch (system_error&) {
                LOG(ERROR) << "Device " << devid << ": truncatePiece failed";
                dev->setState(DistDevice::MISSING);
                files_[i].reset();
                of_[i].reset();
                bad.insert(devid);
            }
        }
        else {
            LOG(ERROR) << "Device " << devid
                       << ": not healthy, removing location";
            bad.insert(devid);
        }
    }
    removeBadLocations(lk, bad, 0s, trans);
}

bool DistPiece::resilverLocation(std::unique_lock<std::mutex>& lk, int which)
{
    Credential cred{0, 0, {}, true};

    // Find some data store with index less than 'which' to copy from
    shared_ptr<DataStore> fromds, tods;
    shared_ptr<File> tofile;
    shared_ptr<OpenFile> toof;

    int count = 0;
    unordered_set<devid> bad;

    assert(files_[which] != 0);
    try {
        tods = fs_.lock()->findDataStore(loc_[which].device);
        tofile = files_[which];
        toof = tofile->open(cred, OpenFlags::WRITE);
    }
    catch (system_error&) {
        LOG(ERROR) << "Can't connect to target device for resilvering";
        return false;
    }

    if (index_ > which)
        index_ = 0;
    while (count < which) {
        auto id = loc_[index_].device;
        try {
            fromds = fs_.lock()->findDataStore(id);

            if (!files_[index_]) {
                // This is the first time we have tried to use this DS - look
                // up the piece now
                files_[index_] = fromds->findPiece(cred, id_);
            }

            LOG(INFO) << "Resilvering " << id_
                      << ": from device " << id
                      << " to device " << loc_[which].device;

            auto fromfile = files_[index_];
            auto fromof = fromfile->open(cred, OpenFlags::READ);

            bool eof = false;
            uint64_t off = 0;
            while (!eof) {
                auto buf = fromof->read(off, 32768, eof);
                if (buf->size()) {
                    toof->write(off, buf);
                    off += buf->size();
                }
            }
            break;
        }
        catch (system_error&) {
            LOG(ERROR) << "Device " << id << ": resilver failed";
            bad.insert(id);
            count++;
            index_++;
            if (index_ ==  which)
                index_ = 0;
        }
    }

    // If we can't resilver, don't just drop the bad source locations.
    // If we have a network partition, that may cause us to discard
    // locations which would still be usable after the partition is
    // restored.

    // Return true if at least one copy succeeded
    return bad.size() < which;
}

void DistPiece::remove(const Credential& cred, Transaction* trans)
{
    auto lk = lock();
    auto fs = fs_.lock();
    for (int i = 0; i < int(loc_.size()); i++) {
        auto& entry = loc_[i];
        //LOG(INFO) << "Removing " << id_ << " from device " << entry.device;
        trans->remove(
            fs->piecesNS(), DoubleKeyType(entry.device, entry.index));
        auto devid = entry.device;
        try {
            auto ds = fs->findDataStore(devid);
            ds->removePiece(cred, id_);
        }
        catch (system_error&) {
            LOG(ERROR) << "Device " << devid
                       << ": remove piece " << id_ << " failed";
        }
    }
}

void DistPiece::removeBadLocation(
    devid id, system_clock::duration delay, Transaction* trans)
{
    auto lk = lock();
    auto fs = fs_.lock();
    unordered_set<devid> bad{id};
    removeBadLocations(lk, bad, delay, trans);
}

void DistPiece::removeBadLocations(
    std::unique_lock<std::mutex>& lk,
    const unordered_set<devid>& bad,
    system_clock::duration delay,
    Transaction* trans)
{
    auto fs = fs_.lock();
    if (bad.size() > 0) {
        if (bad.size() == loc_.size()) {
            // All replicas are bad
            throw system_error(EIO, system_category());
        }
        PieceLocation newloc;
        vector<shared_ptr<File>> newfiles;
        vector<shared_ptr<OpenFile>> newof;
        for (int i = 0; i < int(loc_.size()); i++) {
            auto& entry = loc_[i];
            if (bad.find(entry.device) == bad.end()) {
                newloc.push_back(entry);
                newfiles.push_back(files_[i]);
                newof.push_back(of_[i]);
            }
            else {
                trans->remove(
                    fs->piecesNS(), DoubleKeyType(entry.device, entry.index));
            }
        }

        // Write the new set of locations to the data table
        auto buf = make_shared<Buffer>(oncrpc::XdrSizeof(newloc));
        oncrpc::XdrMemory xm(buf->data(), buf->size());
        xdr(newloc, static_cast<oncrpc::XdrSink*>(&xm));
        PieceData key(id_);
        trans->put(fs->dataNS(), key, buf);
        trans->put(fs->repairsNS(), key, make_shared<Buffer>(0));

        loc_ = move(newloc);
        files_ = move(newfiles);
        of_ = move(newof);

        setState(NEED_RESILVER);
    }
    if (loc_.size() < fs->replicas())
        fs->resilverPiece(id_, delay);
}
