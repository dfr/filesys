#pragma once

#include <fs++/filecache.h>
#include <fs++/filesys.h>
#include "filesys/objfs/objfsproto.h"
#include "filesys/objfs/dbi.h"

namespace filesys {
namespace objfs {

constexpr int OBJFS_NAME_MAX = 255;     // consistent with FreeBSD default

class ObjFilesystem;

/// Key type for our DB - we index by fileid, using fileid zero for filesystem
/// metadata. The fileid is encoded little endian to make best use of bloom
/// filters
struct KeyType {
    KeyType(FileId id)
        : buf_(sizeof(std::uint64_t)),
          fileid_(id)
    {
        std::uint64_t n = id;
        for (int i = 0; i < sizeof(n); i++) {
            buf_.data()[i] = n & 0xff;
            n >>= 8;
        }
    }

    operator const oncrpc::Buffer&() const
    {
        return buf_;
    }

    auto fileid() const { return fileid_; }

private:
    oncrpc::Buffer buf_;
    FileId fileid_;
};

/// Key type for directory entries - we append an index number to the fileid
/// which groups the keys for efficient iteration. We use big endian for
/// the fileid so we can iterate over the directory easily
struct DirectoryKeyType
{
    DirectoryKeyType(FileId id, std::string name)
        : buf_(sizeof(std::uint64_t) + name.size())
    {
        std::uint64_t n = id;
        for (int i = 0; i < sizeof(n); i++) {
            buf_.data()[i] = (n >> 56) & 0xff;
            n <<= 8;
        }
        std::copy_n(
            reinterpret_cast<const uint8_t*>(name.data()), name.size(),
            buf_.data() + sizeof(uint64_t));
    }

    operator const oncrpc::Buffer&() const
    {
        return buf_;
    }

private:
    oncrpc::Buffer buf_;
};

/// Key type for file data and block map - we index by fileid and byte offset,
/// using the highest bit of the file offset to distinguish between data and
/// block map
struct DataKeyType {
    DataKeyType(FileId id, uint64_t off)
        : buf_(2*sizeof(std::uint64_t))
    {
        std::uint64_t n = id;
        for (int i = 0; i < sizeof(n); i++) {
            buf_.data()[i] = n & 0xff;
            n >>= 8;
        }
        n = off;
        for (int i = 0; i < sizeof(n); i++) {
            buf_.data()[8+i] = n & 0xff;
            n >>= 8;
        }
    }

    operator const oncrpc::Buffer&() const
    {
        return buf_;
    }

private:
    oncrpc::Buffer buf_;
};

class ObjGetattr: public Getattr
{
public:
    ObjGetattr(FileId fileid, const PosixAttr& attr, std::uint64_t used)
        : fileid_(fileid),
          attr_(attr),
          used_(used)
    {
    }

    // Getattr overrides
    FileType type() const override;
    int mode() const override;
    int nlink() const override;
    int uid() const override;
    int gid() const override;
    std::uint64_t size() const override;
    std::uint64_t used() const override;
    FileId fileid() const override;
    std::chrono::system_clock::time_point mtime() const override;
    std::chrono::system_clock::time_point atime() const override;
    std::chrono::system_clock::time_point ctime() const override;
    std::chrono::system_clock::time_point birthtime() const override;
    std::uint64_t createverf() const override;

private:
    FileId fileid_;
    PosixAttr attr_;
    std::uint64_t used_;
};

class ObjSetattr: public Setattr
{
public:
    ObjSetattr(const Credential& cred, PosixAttr& attr)
        : cred_(cred), attr_(attr) {}

    // Setattr overrides
    void setMode(int mode) override;
    void setUid(int uid) override;
    void setGid(int gid) override;
    void setSize(std::uint64_t size) override;
    void setMtime(std::chrono::system_clock::time_point mtime) override;
    void setAtime(std::chrono::system_clock::time_point atime) override;
    void setCreateverf(std::uint64_t verf) override;

private:
    const Credential& cred_;
    PosixAttr& attr_;
};

class ObjFsattr: public Fsattr
{
public:
    size_t tbytes() const override { return 0; }
    size_t fbytes() const override { return 0; }
    size_t abytes() const override { return 0; }
    size_t tfiles() const override { return 0; }
    size_t ffiles() const override { return 0; }
    size_t afiles() const override { return 0; }
    int linkMax() const override
    {
        return std::numeric_limits<int>::max();
    }
    int nameMax() const override
    {
        return OBJFS_NAME_MAX;
    }
};

/// Subclass ObjFileMeta to add some helper methods
struct ObjFileMetaImpl: public ObjFileMeta
{
    ObjFileMetaImpl()
    {
        vers = 1;
        fileid = 0;
        attr.type = PT_REG;
        attr.mode = 0;
        attr.nlink = 0;
        attr.uid = 0;
        attr.gid = 0;
        attr.size = 0;
        attr.atime = 0;
        attr.mtime = 0;
        attr.ctime = 0;
        location.set_type(LOC_EMBEDDED);
        location.embedded().data.clear();
    }
    ObjFileMetaImpl(ObjFileMeta&& other)
    {
        fileid = other.fileid;
        attr = other.attr;
        location = std::move(other.location);
    }
};

class ObjFile: public File, public std::enable_shared_from_this<ObjFile>
{
public:
    ObjFile(std::shared_ptr<ObjFilesystem>, FileId fileid);
    ObjFile(std::shared_ptr<ObjFilesystem>, ObjFileMetaImpl&& meta);
    ~ObjFile() override;

    // File overrides
    std::shared_ptr<Filesystem> fs() override;
    void handle(FileHandle& fh) override;
    bool access(const Credential& cred, int accmode) override;
    std::shared_ptr<Getattr> getattr() override;
    void setattr(const Credential& cred, std::function<void(Setattr*)> cb) override;
    std::shared_ptr<File> lookup(const Credential& cred, const std::string& name) override;
    std::shared_ptr<File> open(
        const Credential& cred, const std::string& name, int flags,
        std::function<void(Setattr*)> cb) override;
    void close(const Credential& cred) override;
    void commit(const Credential& cred) override;
    std::string readlink(const Credential& cred) override;
    std::shared_ptr<oncrpc::Buffer> read(
        const Credential& cred, std::uint64_t offset, std::uint32_t size,
        bool& eof) override;
    std::uint32_t write(
        const Credential& cred, std::uint64_t offset,
        std::shared_ptr<oncrpc::Buffer> data) override;
    std::shared_ptr<File> mkdir(
        const Credential& cred, const std::string& name,
        std::function<void(Setattr*)> cb) override;
    std::shared_ptr<File> symlink(
        const Credential& cred, const std::string& name,
        const std::string& data, std::function<void(Setattr*)> cb) override;
    std::shared_ptr<File> mkfifo(
        const Credential& cred, const std::string& name,
        std::function<void(Setattr*)> cb) override;
    void remove(const Credential& cred, const std::string& name) override;
    void rmdir(const Credential& cred, const std::string& name) override;
    void rename(
        const Credential& cred, const std::string& toName,
        std::shared_ptr<File> fromDir,
        const std::string& fromName) override;
    void link(
        const Credential& cred, const std::string& name,
        std::shared_ptr<File> file) override;
    std::shared_ptr<DirectoryIterator> readdir(
        const Credential& cred, std::uint64_t seek) override;
    std::shared_ptr<Fsattr> fsstat(const Credential& cred) override;

    FileId fileid() const { return FileId(meta_.fileid); }
    std::shared_ptr<ObjFile> lookupInternal(
        std::unique_lock<std::mutex>& lock,
        const Credential& cred, const std::string& name);
    void writeMeta();
    void writeMeta(Transaction* trans);
    void writeDirectoryEntry(
        Transaction* trans, const std::string& name, FileId id);
    void link(Transaction* trans, const std::string& name, ObjFile* file,
        bool saveMeta);
    void unlink(Transaction* trans, const std::string& name, ObjFile* file,
        bool saveMeta);
    std::shared_ptr<File> createNewFile(
        std::unique_lock<std::mutex>& lock,
        const Credential& cred,
        PosixType type,
        const std::string& name,
        std::function<void(Setattr*)> attrCb,
        std::function<void(Transaction*, std::shared_ptr<ObjFile>)> writeCb);
    void checkAccess(const Credential& cred, int accmode);
    void checkSticky(const Credential& cred, ObjFile* file);

private:
    std::mutex mutex_;
    std::weak_ptr<ObjFilesystem> fs_;
    ObjFileMetaImpl meta_;
    int fd_ = -1;
};

class ObjDirectoryIterator: public DirectoryIterator
{
public:
    ObjDirectoryIterator(
        std::shared_ptr<ObjFilesystem> fs, FileId fileid, std::uint64_t seek);

    bool valid() const override;
    FileId fileid() const override;
    std::string name() const override;
    std::shared_ptr<File> file() const override;
    uint64_t seek() const override;
    void next() override;

private:
    void decodeEntry();

    std::shared_ptr<ObjFilesystem> fs_;
    std::unique_ptr<Iterator> iterator_;
    uint64_t seek_;
    DirectoryKeyType start_;
    DirectoryKeyType end_;
    DirectoryEntry entry_;
    mutable std::shared_ptr<File> file_;
};

class ObjFilesystem: public Filesystem,
                     public std::enable_shared_from_this<ObjFilesystem>
{
public:
    ObjFilesystem(const std::string& filename);
    ~ObjFilesystem() override;

    std::shared_ptr<File> root() override;
    const FilesystemId& fsid() const override;
    std::shared_ptr<File> find(const FileHandle& fh) override;

    auto defaultNS() const { return defaultNS_; }
    auto directoriesNS() const { return directoriesNS_; }
    auto dataNS() const { return dataNS_; }

    Database* db() const
    {
        return db_.get();
    }

    int blockSize() const
    {
        return meta_.blockSize;
    }

    FileId nextId()
    {
        return FileId(nextId_++);
    }

    std::shared_ptr<ObjFile> find(FileId fileid);
    void remove(FileId fileid);
    void add(std::shared_ptr<ObjFile> file);
    void writeMeta(Transaction* trans);
    void setFsid();

private:
    std::mutex mutex_;
    std::unique_ptr<Database> db_;
    Namespace* defaultNS_;
    Namespace* directoriesNS_;
    Namespace* dataNS_;
    ObjFilesystemMeta meta_;
    std::atomic<std::uint64_t> nextId_;
    FilesystemId fsid_;
    std::shared_ptr<ObjFile> root_;
    detail::FileCache<std::uint64_t, ObjFile> cache_;
};

class ObjFilesystemFactory: public FilesystemFactory
{
public:
    std::string name() const override { return "objfs"; }
    std::pair<std::shared_ptr<Filesystem>, std::string> mount(
        FilesystemManager* fsman, const std::string& url) override;
};

void init(FilesystemManager* fsman);

}
}
