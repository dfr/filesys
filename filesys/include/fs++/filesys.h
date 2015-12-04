#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <rpc++/cred.h>
#include <rpc++/xdr.h>

namespace filesys {

using oncrpc::Credential;

namespace detail {

/// A simple wrapper for system_clock::now which we can mock for testing
class Clock
{
public:
    typedef std::chrono::system_clock::time_point time_point;
    typedef std::chrono::system_clock::duration duration;
    virtual ~Clock() {}
    virtual time_point now() = 0;
};

class SystemClock: public Clock
{
public:
    virtual time_point now()
    {
        return std::chrono::system_clock::now();
    }
};

}

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
    std::vector<std::uint8_t> handle; // filesystem-specific handle
    int operator==(const FileHandle& other) const
    {
        return version == other.version &&
            handle == other.handle;
    }
};

template <typename XDR>
static inline void xdr(oncrpc::RefType<FileHandle, XDR> v, XDR* xdrs)
{
    xdr(v.version, xdrs);
    xdr(v.handle, xdrs);
}

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
};

/// Filesystem attributes
class Fsattr
{
public:
    virtual ~Fsattr() {}
    virtual size_t tbytes() const = 0;
    virtual size_t fbytes() const = 0;
    virtual size_t abytes() const = 0;
    virtual size_t tfiles() const = 0;
    virtual size_t ffiles() const = 0;
    virtual size_t afiles() const = 0;
    virtual int linkMax() const = 0;
    virtual int nameMax() const = 0;
};

/// A file, directory or other filesystem object
class File
{
public:
    virtual ~File() {}

    /// Return the file system that owns this file
    virtual std::shared_ptr<Filesystem> fs() = 0;

    /// Get a file handle for this file
    virtual void handle(FileHandle& fh) = 0;

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
    virtual std::shared_ptr<File> open(
        const Credential& cred, const std::string& name, int flags,
        std::function<void(Setattr*)> cb) = 0;

    /// Close a file previously opened with open
    virtual void close(const Credential& cred) = 0;

    /// Commit cached data to stable storage
    virtual void commit(const Credential& cred) = 0;

    /// Read the contents of a symbolic links
    virtual std::string readlink(const Credential& cred) = 0;

    /// Read data from a file
    virtual std::shared_ptr<oncrpc::Buffer> read(
        const Credential& cred, std::uint64_t offset, std::uint32_t size,
        bool& eof) = 0;

    /// Write data to a file
    virtual std::uint32_t write(
        const Credential& cred, std::uint64_t offset,
        std::shared_ptr<oncrpc::Buffer> data) = 0;

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
};

class FilesystemFactory
{
public:
    virtual std::string name() const = 0;
    virtual std::pair<std::shared_ptr<Filesystem>, std::string> mount(
        FilesystemManager* fsman, const std::string& url) = 0;
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

    template <typename FS, typename... Args>
    std::shared_ptr<FS> mount(const std::string& name, Args... args)
    {
        auto res = std::make_shared<FS>(std::forward<Args>(args)...);
        filesystems_[name] = res;
        return res;
    }

    void unmountAll()
    {
        filesystems_.clear();
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
