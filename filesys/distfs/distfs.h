/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include <set>

#include "filesys/distfs/distfsproto.h"
#include "filesys/nfs4/nfs4ds.h"
#include "filesys/objfs/objfs.h"

namespace filesys {
namespace distfs {

class DistFilesystem;
class DistPiece;

static inline std::ostream& operator<<(
    std::ostream& os, const distfs_owner& owner)
{
    auto savefill = os.fill();
    auto saveflags = os.flags();
    os << "distfs_owner{{";
    for (auto c: owner.do_verifier) {
        os << std::hex << std::setw(2) << std::setfill('0') << int(c);
    }
    os << "},{";
    for (auto c: owner.do_ownerid) {
        os << std::hex << std::setw(2) << std::setfill('0') << int(c);
    }
    os << "}}";
    os.fill(savefill);
    os.flags(saveflags);
    return os;
}

class DistFile: public objfs::ObjFile
{
public:
    DistFile(std::shared_ptr<objfs::ObjFilesystem>, FileId fileid);
    DistFile(
        std::shared_ptr<objfs::ObjFilesystem>, objfs::ObjFileMetaImpl&& meta);
    ~DistFile() override;

    // File overrides
    std::shared_ptr<Fsattr> fsstat(const Credential& cred) override;
    std::shared_ptr<Piece> data(
        const Credential& cred, std::uint64_t offset, bool forWriting) override;

    // ObjFile overrides
    void truncate(
        const Credential& cred, keyval::Transaction* trans,
        std::uint64_t newSize) override;

private:
};

class DistOpenFile: public OpenFile
{
public:
    DistOpenFile(const Credential& cred, std::shared_ptr<DistFile> file, int flags)
        : cred_(cred),
          file_(file),
          flags_(flags)
    {
    }

    ~DistOpenFile() override;

    // DistOpenFile overrides
    std::shared_ptr<File> file() const override { return file_; }
    std::shared_ptr<Buffer> read(
        std::uint64_t offset, std::uint32_t size, bool& eof) override;
    std::uint32_t write(
        std::uint64_t offset, std::shared_ptr<Buffer> data) override;
    void flush() override;

private:
    Credential cred_;
    std::shared_ptr<DistFile> file_;
    int flags_;
    bool needFlush_ = false;
    std::unordered_set<std::shared_ptr<DistPiece>> pieces_;
};

template<typename ARRAY>
static int _djb2(const ARRAY& a, int seed = 5381)
{
    size_t hash = seed;
    for (auto c: a)
        hash = (hash << 5) + hash + c; /* hash * 33 + c */
    return hash;
}

typedef oncrpc::bounded_vector<std::uint8_t, DISTFS_OPAQUE_LIMIT> DSOwnerId;

struct DSOwnerIdHash
{
    size_t operator()(const DSOwnerId& ownerid) const
    {
        return _djb2(ownerid);
    }
};

static inline int operator!=(const distfs_owner& x, const distfs_owner& y)
{
    return x.do_verifier != y.do_verifier || x.do_ownerid != y.do_ownerid;
}

/// Used as keys in the data table and values in the pieces table
class PieceData : public objfs::DataKeyType
{
public:
    PieceData(FileId fileid, std::uint64_t offset, std::uint32_t size)
        : DataKeyType(fileid, offset | l2size(size))
    {
        assert((offset & 127) == 0);
    }

    PieceData(const PieceId& id)
        : PieceData(id.fileid, id.offset, id.size)
    {
    }

    PieceData(std::shared_ptr<oncrpc::Buffer> buf)
        : DataKeyType(buf)
    {
    }

    std::uint64_t offset() const
    {
        return DataKeyType::offset() & ~127;
    }

    std::uint32_t size() const
    {
        auto sz = DataKeyType::offset() & 127;
        if (sz == 64)
            return 0u;
        assert(sz < 32);
        return 1u << sz;
    }

private:
    static inline int l2size(uint32_t val)
    {
        // Special case size 0, which we use to mean 1<<64
        if (val == 0)
            return 64;
        int i = __builtin_ffs(val);
        assert(val == (1u << (i-1)));
        return i-1;
    }
};

struct PieceIdHash
{
    size_t operator()(const PieceId& id) const
    {
        std::hash<uint64_t> h;
        return h(id.fileid) + h(id.offset) + h(id.size);
    }
};

static inline std::ostream& operator<<(std::ostream& os, const PieceId& id)
{
    os << "{" << id.fileid << "," << id.offset << "," << id.size << "}";
    return os;
}

class DistPiece: public Piece,
                 public std::enable_shared_from_this<DistPiece>
{
public:
    enum State {
        /// Piece is healthy and there are no external clients with
        /// access to the piece
        IDLE,

        /// Piece is healthy and there are external clients (e.g. pNFS
        /// layouts)
        BUSY,

        /// Piece has external clients which have been recalled so
        /// that the piece can be modified in some way
        /// (e.g. resilvered or removed)
        RECALLING,

        /// Piece needs to be resilvered to new locations
        NEED_RESILVER,

        /// Piece is currently resilvering
        RESILVERING,
    };

    /// Create a new piece with no current locations
    DistPiece(std::shared_ptr<DistFilesystem> fs, PieceId id)
        : fs_(fs),
          id_(id),
          state_(IDLE),
          index_(0),
          targetCopies_(0)
    {
    }

    /// Create a new piece with the given set of locations
    DistPiece(
        std::shared_ptr<DistFilesystem> fs, PieceId id,
        const PieceLocation& loc)
        : fs_(fs),
          id_(id),
          state_(IDLE),
          index_(0),
          targetCopies_(loc.size()),
          loc_(loc),
          files_(loc.size()),
          of_(loc.size())
    {
    }

    // Piece overrides
    PieceId id() const override;
    int mirrorCount() const override;
    std::pair<std::shared_ptr<Device>, std::shared_ptr<File>> mirror(
        const Credential& cred, int i) override;

    auto lock()
    {
        return std::unique_lock<std::mutex>(mutex_);
    }

    auto& loc() const { return loc_; }

    /// Set the current state of the piece
    void setState(State state);

    /// Add a set of new locations for the piece. This is called when
    /// the piece is created to give it an initial set of locations
    /// and during resilvering to migrate the piece to a new device,
    void addPieceLocations(
        const PieceLocation& loc,
        std::vector<std::shared_ptr<File>>&& files, bool resilver,
        keyval::Transaction* trans);

    /// Read data from the piece
    std::shared_ptr<Buffer> read(const Credential& cred, int off, int sz);

    /// Write data to all copies of the piece
    int write(
        const Credential& cred, int off, std::shared_ptr<Buffer> buf,
        keyval::Transaction* trans);

    /// Drop all open-file objects
    void close();

    /// Truncate the piece to the given size
    void truncate(
        const Credential& cred, int newSize, keyval::Transaction* trans);

    /// Remove the piece, deleting it from all devices
    void remove(const Credential& cred, keyval::Transaction* trans);

    /// Resilver data from existing devices to the device indicated by
    /// 'which'
    bool resilverLocation(std::unique_lock<std::mutex>& lk, int which);

    /// Remove a device from the list of locations for the piece,
    /// arranging to resilver the piece after the given delay
    void removeBadLocation(
        devid id, std::chrono::system_clock::duration delay,
        keyval::Transaction* trans);

    /// Remove a set of devices from the list of locations for the
    /// piece, arranging to resilver the piece after the given delay
    void removeBadLocations(
        std::unique_lock<std::mutex>& lk,
        const std::unordered_set<devid>& bad,
        std::chrono::system_clock::duration delay,
        keyval::Transaction* trans);

    /// Return true if the piece includes id in its list of valid
    /// locations
    bool hasLocation(devid id)
    {
        for (auto& entry: loc_)
            if (entry.device == id)
                return true;
        return false;
    }

private:
    /// Owning filesystem
    std::weak_ptr<DistFilesystem> fs_;

    /// Unique id of this piece
    PieceId id_;

    /// Protects fields listed below
    std::mutex mutex_;

    /// Current state of the piece
    State state_;

    /// Index of which device to read from
    int index_;

    /// The target number of copies of the piece - may be greater then
    /// the current set of locations in the case when a piece needs
    /// resilvering
    int targetCopies_;

    /// List of devices containing copies of the piece
    PieceLocation loc_;

    /// Cache file objects for copies of the piece
    std::vector<std::shared_ptr<File>> files_;

    /// Cache open-file objects for i/o to the piece
    std::vector<std::shared_ptr<OpenFile>> of_;
};

/// We maintain an instance of this for each potential data store
class DistDevice: public Device,
                  public std::enable_shared_from_this<DistDevice>
{
public:

    DistDevice(
        int id, const DeviceStatus& status);
    DistDevice(
        int id, const DeviceStatus& status, const StorageStatus& storage);

    // Device overrides
    uint64_t id() const override { return id_; }
    Device::State state() const override { return state_; }
    std::vector<oncrpc::AddressInfo> addresses() const override;
    CallbackHandle addStateCallback(std::function<void(State)> cb) override;
    void removeStateCallback(CallbackHandle h) override;

    auto owner() const { return owner_; }
    //auto& uaddrs() const { return uaddrs_; }
    auto& addrs() const { return addrs_; }
    auto& storage() const { return storage_; }
    auto priority() const { return priority_; }

    auto lock() const
    {
        // Note: this can be const because mutex_ is mutable.
        return std::unique_lock<std::mutex>(mutex_);
    }

    /// Update a device entry, returning true if anything changed
    bool update(
        std::weak_ptr<DistFilesystem> fs,
        std::shared_ptr<oncrpc::TimeoutManager> tman,
        const DeviceStatus& status, const StorageStatus& storage);

    void calculatePriority();

    void setPriority(float priority)
    {
        priority_ = priority;
    }

    void setStorage(const StorageStatus& storage)
    {
        storage_ = storage;
    }

    void clearStorage()
    {
        storage_ = {0, 0, 0};
    }

    void write(std::shared_ptr<DistFilesystem> fs);

    void setState(State state)
    {
        auto lk = lock();
        setState(lk, state);
    }
    void setState(std::unique_lock<std::mutex>& lk, State state);

    void setNextPieceIndex(uint64_t index)
    {
        nextPieceIndex_ = index;
    }

    uint64_t nextPieceIndex() const
    {
        return nextPieceIndex_;
    }

    uint64_t newPieceIndex()
    {
        return nextPieceIndex_++;
    }

    /// Schedule a task to evaluate a data device's state
    void scheduleTimeout(
        std::weak_ptr<DistFilesystem> fs,
        std::shared_ptr<oncrpc::TimeoutManager> tman);

private:
    void resolveAddresses();

    int id_;
    mutable std::mutex mutex_;
    distfs_owner owner_;
    std::vector<std::string> uaddrs_;
    std::vector<oncrpc::AddressInfo> addrs_;
    StorageStatus storage_;
    float priority_;
    uint64_t nextPieceIndex_;
    State state_;
    oncrpc::TimeoutManager::task_type timeout_ = 0;
    CallbackHandle nextCbHandle_ = 1;
    std::unordered_map<
        CallbackHandle, std::function<void(State)>> callbacks_;
};

struct DevCompare
{
    int operator()(
        std::shared_ptr<DistDevice> x, std::shared_ptr<DistDevice> y)
    {
        if (x->priority() == y->priority())
            return x->id() < y->id();
        return x->priority() < y->priority();
    }
};

class DistFsattr: public objfs::ObjFsattr
{
public:
    DistFsattr(std::shared_ptr<DistFilesystem> fs);

    size_t totalSpace() const override;
    size_t freeSpace() const override;
    size_t availSpace() const override;
    int repairQueueSize() const override;

private:
    const StorageStatus storage_;
    int repairQueueSize_;
};

class DistFilesystem: public objfs::ObjFilesystem,
                      public DistfsMds1Service
{
public:
    DistFilesystem(
        std::shared_ptr<keyval::Database> db,
        const std::string& addr,
        std::shared_ptr<detail::Clock> clock);
    DistFilesystem(
        std::shared_ptr<keyval::Database> db,
        const std::string& addr);
    ~DistFilesystem() override;

    // Filesystem overrides
    bool isMetadata() const override { return true; }
    std::vector<std::shared_ptr<Device>> devices(std::uint64_t& gen) override;
    std::shared_ptr<Device> findDevice(std::uint64_t& devid) override;

    // ObjFilesystem overrides
    std::shared_ptr<objfs::ObjFile> makeNewFile(FileId fileid) override;
    std::shared_ptr<objfs::ObjFile> makeNewFile(
        objfs::ObjFileMetaImpl&& meta) override;
    std::shared_ptr<OpenFile> makeNewOpenFile(
        const Credential& cred, std::shared_ptr<objfs::ObjFile> file, int flags) override;

    // Distfs MDS protocol
    void null() override {}
    void status(const STATUSargs& args) override;

    auto shared_from_this() {
        return std::dynamic_pointer_cast<DistFilesystem>(
            ObjFilesystem::shared_from_this());
    }
    auto devicesNS() const { return devicesNS_; }
    auto piecesNS() const { return piecesNS_; }
    auto repairsNS() const { return repairsNS_; }
    auto storage() const { return storage_; }
    auto replicas() const { return replicas_; }
    auto repairQueueSize() const { return repairQueueSize_; }

#if 0
    /// Assuming a 10 Pb filesystem with 10 Mb pieces, we need to
    /// track up to 100 million pieces. Assuming conservatively that
    /// we need 200 bytes of metadata per piece, this comes to about
    /// 200 Gb of metadata which seems reasonable.
    auto pieceSize() const { return 1024 * 1024; }
#else
    /// For compatibility with Linux flex files client, we use a
    /// default piece size of zero which means that each file has
    /// exactly one piece
    auto pieceSize() const { return 0; }
#endif

    /// Look up a data device by id
    std::shared_ptr<DistDevice> lookupDevice(devid id)
    {
        return devicesById_[id];
    }

    /// Return an object which can be used to access the data for a
    /// piece of a file. If there is no existing piece and create is
    /// true, make a new piece, writing piece metadata to the given
    /// transaction
    std::shared_ptr<DistPiece> findPiece(
        PieceId id, bool create, keyval::Transaction* trans);

    /// Delete a data piece from all the devices associated with
    /// it
    void removePiece(
        const Credential& cred, PieceId id, keyval::Transaction* trans);

    /// Schedule resilvering a data piece after we lose contact with a
    /// device
    void resilverPiece(PieceId id, std::chrono::system_clock::duration delay);

    /// Create count locations for new copies of the given piece. If
    /// resilver is true, data for the new piece initialised with data
    /// copied from an existing replica.
    void addPieceLocations(
        std::shared_ptr<DistPiece> piece, int count, bool resilver,
        keyval::Transaction* trans);

    /// Return a object which can communicate with the data device
    /// with the given id
    std::shared_ptr<DataStore> findDataStore(devid id);

    /// For unit testing, manually add a data store
    void addDataStore(std::shared_ptr<DataStore> ds);

    /// Restore a recently restarted data device, validating its piece
    /// collection
    void restoreDevice(std::shared_ptr<DistDevice> dev);

    /// If a data device stops sending updates for long enough
    /// (currently ten times the heartbeat delay), we assume it has
    /// failed permanently and we arrange to resilver its piece
    /// collection
    void decommissionDevice(std::shared_ptr<DistDevice> dev);

private:
    /// Unique client owner string for connecting to devices using
    /// NFSv4
    std::string clientowner_;

    /// Binding address
    std::string addr_;

    /// Number of replicas of each piece of data to store
    int replicas_;

    /// This namespace contains details of all known data devices,
    /// indexed by device ID
    std::shared_ptr<keyval::Namespace> devicesNS_;

    /// A namespace containing all known piece locations, indexed by a
    /// pair made up from the device ID and an integer uniquely
    /// identifying the piece on that device. Values are the
    /// PieceId. This is used to detect situations where we beleive
    /// that a data device contains a copy of a data piece but the
    /// device itself doesn't have that piece
    std::shared_ptr<keyval::Namespace> piecesNS_;

    /// A namespace to keep track of pieces which are resilvering. Keys are
    /// PieceData and values are empty
    std::shared_ptr<keyval::Namespace> repairsNS_;

    /// Storage summary
    StorageStatus storage_ = {0, 0, 0};
    int repairQueueSize_ = 0;

    int nextDeviceId_ = 1;
    std::unordered_map<DSOwnerId,
                       std::shared_ptr<DistDevice>,
                       DSOwnerIdHash> devicesByOwnerId_;
    std::map<devid, std::shared_ptr<DistDevice>> devicesById_;
    std::set<std::shared_ptr<DistDevice>, DevCompare> devices_;
    std::set<std::shared_ptr<DistDevice>, DevCompare> devicesToRestore_;
    std::uint64_t devicesGen_ = 1;

    // Device to device protocol support
    std::thread thread_;
    std::shared_ptr<oncrpc::SocketManager> sockman_;
    std::shared_ptr<oncrpc::ServiceRegistry> svcreg_;

    // Cache piece lookups
    detail::LRUCache<PieceId, DistPiece, PieceIdHash> piececache_;

    // Cache connections to devices
    detail::LRUCache<devid, DataStore> dscache_;
};

class DistFilesystemFactory: public FilesystemFactory
{
public:
    std::string name() const override { return "distfs"; }
    std::shared_ptr<Filesystem> mount(const std::string& url) override;
};

void init(FilesystemManager* fsman);

}
}
