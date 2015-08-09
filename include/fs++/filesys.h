#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace filesys {

class File;

/// Possible file types
enum class FileType {
    FILE,
    DIRECTORY,
    BLOCKDEV,
    CHARDEV,
    SYMLINK,
    SOCKET,
    BAD,
    FIFO
};

/// Flags for File::open
struct OpenFlags
{
    static constexpr int READ = 1;
    static constexpr int WRITE = 2;
    static constexpr int CREATE = 4;
    static constexpr int TRUNCATE = 8;
    static constexpr int EXCLUSIVE = 16;
};

/// Iterate over the contents of a directory
class DirectoryIterator
{
public:
    /// Return true if the iterator points at a valid directory entry
    virtual bool valid() const = 0;

    /// Return the current entry's file id
    virtual std::uint64_t fileid() const = 0;

    /// Return the current entry's file name
    virtual std::string name() const = 0;

    /// Return a file object matching the current entry
    virtual std::shared_ptr<File> file() const = 0;

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
    virtual std::uint64_t fileid() const = 0;

    /// Return the time the file was last modified
    virtual std::chrono::system_clock::time_point mtime() const = 0;

    /// Return the time the file was last accessed
    virtual std::chrono::system_clock::time_point atime() const = 0;

    /// Return the time the file attributes last changed
    virtual std::chrono::system_clock::time_point ctime() const = 0;
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

/// A file, directory or other filesystem object
class File
{
public:
    virtual ~File() {}

    /// Return an object which can be used to access the file attributes
    virtual std::shared_ptr<Getattr> getattr() = 0;

    /// Set the file attributes
    virtual void setattr(std::function<void(Setattr*)> cb) = 0;

    /// Look up a name in a directory
    virtual std::shared_ptr<File> lookup(const std::string& name) = 0;

    /// Open or create a file using a combination of OpenFlags
    virtual std::shared_ptr<File> open(
        const std::string& name, int flags,
        std::function<void(Setattr*)> cb) = 0;

    /// Close a file previously opened with open
    virtual void close() = 0;

    /// Commit cached data to stable storage
    virtual void commit() = 0;

    /// Read the contents of a symbolic links
    virtual std::string readlink() = 0;

    /// Read data from a file
    virtual std::vector<std::uint8_t> read(
        std::uint64_t offset, std::uint32_t size, bool& eof) = 0;

    /// Write data to a file
    virtual std::uint32_t write(
        std::uint64_t offset, const std::vector<std::uint8_t>& data) = 0;

    /// Create a new directory
    virtual std::shared_ptr<File> mkdir(
        const std::string& name,
        std::function<void(Setattr*)> cb) = 0;

    /// Return an iterator object which can be used to read the contents of
    /// a directory
    virtual std::shared_ptr<DirectoryIterator> readdir() = 0;
};

class Filesystem
{
public:
    virtual ~Filesystem() {}

    /// Return the root directory of the filesystem
    virtual std::shared_ptr<File> root() = 0;
};

class FilesystemFactory
{
public:
    virtual std::shared_ptr<Filesystem> mount(const std::string& url) = 0;
};

}
