/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <rpc++/cred.h>
#include <rpc++/xdr.h>
#include <rpc++/socket.h>

namespace keyval {
class Database;
}

namespace filesys {

using oncrpc::Credential;
using oncrpc::Buffer;

class File;
class Filesystem;
class FilesystemManager;

/// A unique identifier for a file in some filesystem
class FileId
{
public:
    FileId() : id_(0) {}
    explicit FileId(std::uint64_t id) : id_(id) {}

    auto id() const { return id_; }
    operator std::uint64_t() const { return id_; }

private:
    std::uint64_t id_;
};

/// A unique identifier for a filesystem
typedef std::vector<std::uint8_t> FilesystemId;

/// Possible file types
enum class FileType {
    FILE,
    DIRECTORY,
    BLOCKDEV,
    CHARDEV,
    SYMLINK,
    SOCKET,
    FIFO
};

/// Flags for File::open
struct OpenFlags
{
    static constexpr int READ = 1;
    static constexpr int WRITE = 2;
    static constexpr int RDWR = 3;
    static constexpr int CREATE = 4;
    static constexpr int TRUNCATE = 8;
    static constexpr int EXCLUSIVE = 16;
    static constexpr int SHLOCK = 32;
    static constexpr int EXLOCK = 64;
};

/// File modes
struct ModeFlags
{
    static constexpr int SETUID = 04000;
    static constexpr int SETGID = 02000;
    static constexpr int STICKY = 01000;

    static constexpr int RUSER = 0400;
    static constexpr int WUSER = 0200;
    static constexpr int XUSER = 0100;

    static constexpr int RGROUP = 0040;
    static constexpr int WGROUP = 0020;
    static constexpr int XGROUP = 0010;

    static constexpr int ROTHER = 0004;
    static constexpr int WOTHER = 0002;
    static constexpr int XOTHER = 0001;
};

/// Flags for File::access and CheckAccess
struct AccessFlags
{
    static constexpr int READ = 1;
    static constexpr int WRITE = 2;
    static constexpr int EXECUTE = 4;
    static constexpr int ALL = 7;
};

/// A structure which uniquely identifies a file
struct FileHandle
{
    int version = 1;

    /// filesystem-specific handle
    oncrpc::bounded_vector<std::uint8_t, 128> handle;

    int operator==(const FileHandle& other) const
    {
        return version == other.version &&
            handle == other.handle;
    }
};

extern void xdr(const FileHandle& fh, oncrpc::XdrSink* xdrs);
extern void xdr(FileHandle& fh, oncrpc::XdrSource* xdrs);

/// Iterate over the contents of a directory
class DirectoryIterator
{
public:
    /// Return true if the iterator points at a valid directory entry
    virtual bool valid() const = 0;

    /// Return the current entry's file id
    virtual FileId fileid() const = 0;

    /// Return the current entry's file name
    virtual std::string name() const = 0;

    /// Return a file object matching the current entry
    virtual std::shared_ptr<File> file() const = 0;

    /// A seek cookie that can be used to start a new directory iteration
    /// at the next entry following this one
    virtual std::uint64_t seek() const = 0;

    /// Advance the iterator to the next directory entry (if any)
    virtual void next() = 0;
};

/// Access the attributes of a file
class Getattr
{
public:
    virtual ~Getattr() {}

    /// Return the file type
    virtual FileType type() const = 0;

    /// Return the file mode
    virtual int mode() const = 0;

    /// Return the number of hard links
    virtual int nlink() const = 0;

    /// Return the uid of the file owner
    virtual int uid() const = 0;

    /// Return the gid of the file group
    virtual int gid() const = 0;

    /// Return the file size in bytes
    virtual std::uint64_t size() const = 0;

    /// Return the amount of space used by the file in bytes
    virtual std::uint64_t used() const = 0;

    /// Return optimum block size for i/o
    virtual std::uint32_t blockSize() const = 0;

    /// XXX specdata?

    /// Return the file id
    virtual FileId fileid() const = 0;

    /// Return the time the file was last modified
    virtual std::chrono::system_clock::time_point mtime() const = 0;

    /// Return the time the file was last accessed
    virtual std::chrono::system_clock::time_point atime() const = 0;

    /// Return the time the file attributes last changed
    virtual std::chrono::system_clock::time_point ctime() const = 0;

    /// Return the time the file was created
    virtual std::chrono::system_clock::time_point birthtime() const = 0;

    /// Return a monotonically increasing value which changes when
    /// either file data or metadata changes
    virtual std::uint64_t change() const = 0;

    /// Create verifier used for NFS exclusive create semantics - may
    /// be overlaid with some other metadata (typically atime)
    virtual std::uint64_t createverf() const = 0;
};

/// Settable attributes
class Setattr
{
public:
    /// Set the file mode
    virtual void setMode(int mode) = 0;

    /// Set the file owner
    virtual void setUid(int uid) = 0;

    /// Set the file group
    virtual void setGid(int gid) = 0;

    /// Set the file size
    virtual void setSize(std::uint64_t size) = 0;

    /// Set the file modification time
    virtual void setMtime(std::chrono::system_clock::time_point mtime) = 0;

    /// Set the file access time
    virtual void setAtime(std::chrono::system_clock::time_point atime) = 0;

    /// Set the file change value
    virtual void setChange(std::uint64_t change) = 0;

    /// Set the create verifier used for NFS exclusive create semantics.
    /// May be overlaid with some other metadata (typically atime)
    virtual void setCreateverf(std::uint64_t verf) = 0;
};

/// Filesystem attributes
class Fsattr
{
public:
    virtual ~Fsattr() {}

    /// Total filesystem capacity in bytes
    virtual size_t totalSpace() const = 0;

    /// Space free in bytes
    virtual size_t freeSpace() const = 0;

    /// Space available for uses with the current credential in bytes
    virtual size_t availSpace() const = 0;

    /// Maximum number of files in filesystem
    virtual size_t totalFiles() const = 0;

    /// Number of free files in filesystem
    virtual size_t freeFiles() const = 0;

    /// Available capacity for new files for the current credential
    virtual size_t availFiles() const = 0;

    /// Maximum size of symbolic link data
    virtual int linkMax() const = 0;

    /// Maximum length of a file name
    virtual int nameMax() const = 0;

    /// For fault tolerant filesystems, a count of how many work items
    /// are queued to repair any current defects.
    virtual int repairQueueSize() const = 0;
};

/// A stateful object for performing i/o on a File
class OpenFile
{
public:
    virtual ~OpenFile() {}

    /// Return the File that this object accesses
    virtual std::shared_ptr<File> file() const = 0;

    /// Read data from the file
    virtual std::shared_ptr<Buffer> read(
        std::uint64_t offset, std::uint32_t size, bool& eof) = 0;

    /// Write data to the file
    virtual std::uint32_t write(
        std::uint64_t offset, std::shared_ptr<Buffer> data) = 0;

    /// Write any cached data to stable storage
    virtual void flush() = 0;
};

/// For distributed filesystems, the device object holds information
/// about one node in the data storage network
class Device
{
public:
    /// Device status
    enum State {
        /// We set the state to this on MDS startup.
        UNKNOWN,

        /// The data device has restarted, we are validating its piece
        /// collection.
        RESTORING,

        /// Device has not generated a heartbeat for 2*heartbeat
        /// seconds or messages send to it have failed. We steer
        /// traffic away from it and if a piece in its collection is
        /// written to, we resilver it to some other device.
        MISSING,

        /// Device has not generated a heartbeat for 10*heartbeat
        /// seconds. We resilver its entire piece collection and
        /// delete it from the devices table.
        DEAD,

        /// Device is alive and is producing regular heartbeats.
        HEALTHY,

        /// Pseudo status used to notify address changes
        ADDRESS_CHANGED
    };

    /// An opaque handle used to identify callbacks
    typedef std::uintptr_t CallbackHandle;

    virtual ~Device() {}

    /// A unique identifier for this device
    virtual uint64_t id() const = 0;

    /// Current device state
    virtual State state() const = 0;

    /// Return a list of network addresses for this device's data
    virtual std::vector<oncrpc::AddressInfo> addresses() const = 0;

    /// Return a list of network addresses for this device's admin UI
    virtual std::vector<oncrpc::AddressInfo> adminAddresses() const = 0;

    /// Register a callback which is called if the device changes states
    virtual CallbackHandle addStateCallback(
        std::function<void(State)> cb) = 0;

    /// Remove a state callback
    virtual void removeStateCallback(CallbackHandle h) = 0;
};

struct PieceId
{
    FileId fileid;              // File identifier in owning filesystem
    std::uint64_t offset;       // piece offset in file
    std::uint32_t size;         // piece size
};

static inline int operator==(const PieceId& x, const PieceId& y)
{
    return x.fileid == y.fileid && x.offset == y.offset && x.size == y.size;
}

static inline int operator<(const PieceId& x, const PieceId& y)
{
    if (x.fileid < y.fileid)
        return true;
    else if (x.fileid == y.fileid) {
        if (x.offset < y.offset)
            return true;
        else if (x.offset == y.offset)
            return x.size < y.size;
    }
    return false;
}

/// An object describing how to access part of a file
class Piece
{
public:
    virtual ~Piece() {}

    /// Return the piece identifier
    virtual PieceId id() const = 0;

    /// Return the number of locations which have copies of the piece
    virtual int mirrorCount() const = 0;

    /// Return a {device,file} pair which describes the location of
    /// one mirror
    virtual std::pair<std::shared_ptr<Device>, std::shared_ptr<File>>
        mirror(const Credential& cred, int i) = 0;
};

/// A file, directory or other filesystem object
class File
{
public:
    virtual ~File() {}

    /// Return the file system that owns this file
    virtual std::shared_ptr<Filesystem> fs() = 0;

    /// Get a file handle for this file
    virtual FileHandle handle() = 0;

    /// Return true if the file permissions will allow the requested.
    /// The value of accmode should be a logical-or of AccessFlags values.
    virtual bool access(const Credential& cred, int accmode) = 0;

    /// Return an object which can be used to access the file attributes
    virtual std::shared_ptr<Getattr> getattr() = 0;

    /// Set the file attributes
    virtual void setattr(
        const Credential& cred, std::function<void(Setattr*)> cb) = 0;

    /// Look up a name in a directory
    virtual std::shared_ptr<File> lookup(
        const Credential& cred, const std::string& name) = 0;

    /// Open or create a file using a combination of OpenFlags
    virtual std::shared_ptr<OpenFile> open(
        const Credential& cred, const std::string& name, int flags,
        std::function<void(Setattr*)> cb) = 0;

    /// Get an OpenFile handle to an existing file
    virtual std::shared_ptr<OpenFile> open(
        const Credential& cred, int flags) = 0;

    /// Read the contents of a symbolic links
    virtual std::string readlink(const Credential& cred) = 0;

    /// Create a new directory
    virtual std::shared_ptr<File> mkdir(
        const Credential& cred, const std::string& name,
        std::function<void(Setattr*)> cb) = 0;

    /// Create a new directory
    virtual std::shared_ptr<File> symlink(
        const Credential& cred, const std::string& name,
        const std::string& data, std::function<void(Setattr*)> cb) = 0;

    /// Create a new directory
    virtual std::shared_ptr<File> mkfifo(
        const Credential& cred, const std::string& name,
        std::function<void(Setattr*)> cb) = 0;

    /// Remove a file
    virtual void remove(const Credential& cred, const std::string& name) = 0;

    /// Remove a directory
    virtual void rmdir(const Credential& cred, const std::string& name) = 0;

    /// Rename a file or directory
    virtual void rename(
        const Credential& cred, const std::string& toName,
        std::shared_ptr<File> fromDir,
        const std::string& fromName) = 0;

    /// Link an existing file to this directory
    virtual void link(
        const Credential& cred, const std::string& name,
        std::shared_ptr<File> file) = 0;

    /// Return an iterator object which can be used to read the contents of
    /// a directory. The value of seek should be either zero to start the
    /// iterator at the start of the directory or some value returned by
    /// DirectoryIterator::seek in a previous iteration over this directory.
    virtual std::shared_ptr<DirectoryIterator> readdir(
        const Credential& cred, std::uint64_t seek) = 0;

    /// Return file system attributes
    virtual std::shared_ptr<Fsattr> fsstat(const Credential& cred) = 0;

    /// Return a piece object which describes the location of the data
    /// for part of the file.  If there is no storage allocated for
    /// the given offset and forWriting is false, an ENOENT
    /// system_error is thrown, otherwise a new piece is allocated and
    /// returned.
    virtual std::shared_ptr<Piece> data(
        const Credential& cred, std::uint64_t offset, bool forWriting)
    {
        throw std::system_error(EOPNOTSUPP, std::system_category());
    }
};

class Filesystem
{
public:
    virtual ~Filesystem() {}

    /// Return the root directory of the filesystem
    virtual std::shared_ptr<File> root() = 0;

    /// Return a filesystem id
    virtual const FilesystemId& fsid() const = 0;

    /// Find a file given a file haldne
    virtual std::shared_ptr<File> find(const FileHandle& fh) = 0;

    /// Return true if this is a metadata filesystem (e.g. data is
    /// stored elsewhere and accessed using some network protocol)
    virtual bool isMetadata() const { return false; }

    /// Return true if this is a data filesystem (e.g. this file
    /// system only stores data blocks with metadata accessed
    /// elsewhere in a corresponding metadata filesystem).
    virtual bool isData() const { return false; }

    /// Return a list of devices that forms the backing store of this
    /// filesystem. In order to detect changes in the device list, we
    /// also return a generation number which changes whenever a
    /// device is added or removed.
    virtual std::vector<std::shared_ptr<Device>> devices(std::uint64_t& gen) {
        gen = 0;
        return {};
    }

    /// Find a device given its id
    virtual std::shared_ptr<Device> findDevice(std::uint64_t& devid)
    {
        throw std::system_error(ENOENT, std::system_category());
    }

    /// For filesystems which are backed by a key/value database,
    /// return the database object, otherwise return nullptr
    virtual std::shared_ptr<keyval::Database> database() const
    {
        return nullptr;
    }
};

class DataStore: public Filesystem
{
public:
    /// Return a file object which can be used to access a segment
    /// of the file identified by fileid, block size and block index
    virtual std::shared_ptr<File> findPiece(
        const Credential& cred, const PieceId& id) = 0;

    /// Create a data piece for the given fileid and offset, returning
    /// a file object which can be used to access the new piece.
    virtual std::shared_ptr<File> createPiece(
        const Credential& cred, const PieceId& id) = 0;

    /// Delete a data piece
    virtual void removePiece(
        const Credential& cred, const PieceId& id) = 0;

    /// Schedule regular status reporting with a metadata server
    void reportStatus(
        std::weak_ptr<oncrpc::SocketManager> sockman,
        const std::string& mds,
        const std::vector<oncrpc::AddressInfo>& addrs,
        const std::vector<oncrpc::AddressInfo>& adminAddrs);
};

class FilesystemFactory
{
public:
    virtual std::string name() const = 0;
    virtual std::shared_ptr<Filesystem> mount(const std::string& url) = 0;
};

class FilesystemManager
{
public:
    FilesystemManager();

    static FilesystemManager& instance()
    {
        static FilesystemManager fsman;
        return fsman;
    }

    void mount(const std::string& name, std::shared_ptr<Filesystem> fs)
    {
        filesystems_[name] = fs;
    }

    void add(std::shared_ptr<FilesystemFactory> fsfac)
    {
        factories_[fsfac->name()] = fsfac;
    }

    std::shared_ptr<FilesystemFactory> find(const std::string& name)
    {
        auto i = factories_.find(name);
        if (i == factories_.end())
            return nullptr;
        return i->second;
    }

    std::shared_ptr<File> find(const FileHandle& fh);

    auto begin() { return filesystems_.begin(); }
    auto end() { return filesystems_.end(); }

private:
    std::map<std::string, std::shared_ptr<FilesystemFactory>> factories_;
    std::map<std::string, std::shared_ptr<Filesystem>> filesystems_;
};

/// Check access permissions for the object with the given attributes. If
/// access is denied, a std;:system_error with appropriate error code is
/// thrown.
void CheckAccess(
    int uid, int gid, int mode,const Credential& cred, int accmode);

}
