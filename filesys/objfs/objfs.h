/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include <atomic>

#include <fs++/filesys.h>
#include <fs++/lrucache.h>
#include <kv++/keyval.h>
#include "filesys/objfs/objfsproto.h"
#include "filesys/objfs/objfskey.h"

namespace filesys {
namespace objfs {

constexpr int OBJFS_NAME_MAX = 255;     // consistent with FreeBSD default

class ObjFilesystem;

class ObjGetattr: public Getattr
{
public:
    ObjGetattr(FileId fileid, const PosixAttr& attr, std::uint32_t blockSize,
               std::function<std::uint64_t()> used)
        : fileid_(fileid),
          attr_(attr),
          blockSize_(blockSize),
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
    std::uint32_t blockSize() const override;
    FileId fileid() const override;
    std::chrono::system_clock::time_point mtime() const override;
    std::chrono::system_clock::time_point atime() const override;
    std::chrono::system_clock::time_point ctime() const override;
    std::chrono::system_clock::time_point birthtime() const override;
    std::uint64_t change() const override;
    std::uint64_t createverf() const override;

private:
    FileId fileid_;
    PosixAttr attr_;
    std::uint32_t blockSize_;
    std::function<std::uint64_t()> used_;
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
    void setChange(std::uint64_t change) override;
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
        attr.birthtime = 0;
    }
    ObjFileMetaImpl(ObjFileMeta&& other)
    {
        vers = 1;
        fileid = other.fileid;
        attr = other.attr;
        extra = std::move(other.extra);
    }
};

class ObjFile: public File, public std::enable_shared_from_this<ObjFile>
{
    friend class ObjOpenFile;
public:
    ObjFile(std::shared_ptr<ObjFilesystem>, FileId fileid);
    ObjFile(std::shared_ptr<ObjFilesystem>, ObjFileMetaImpl&& meta);
    ~ObjFile() override;

    std::unique_lock<std::mutex> lock()
    {
        return std::unique_lock<std::mutex>(mutex_);
    }

    // File overrides
    std::shared_ptr<Filesystem> fs() override;
    void handle(FileHandle& fh) override;
    bool access(const Credential& cred, int accmode) override;
    std::shared_ptr<Getattr> getattr() override;
    void setattr(const Credential& cred, std::function<void(Setattr*)> cb) override;
    std::shared_ptr<File> lookup(const Credential& cred, const std::string& name) override;
    std::shared_ptr<OpenFile> open(
        const Credential& cred, const std::string& name, int flags,
        std::function<void(Setattr*)> cb) override;
    std::shared_ptr<OpenFile> open(
        const Credential& cred, int flags) override;
    std::string readlink(const Credential& cred) override;
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

    std::shared_ptr<ObjFilesystem> ofs() const { return fs_.lock(); }
    FileId fileid() const { return FileId(meta_.fileid); }
    auto& meta() const { return meta_; }
    std::shared_ptr<ObjFile> lookupInternal(
        std::unique_lock<std::mutex>& lock,
        const Credential& cred, const std::string& name);
    void writeMeta();
    void writeMeta(keyval::Transaction* trans);
    void writeDirectoryEntry(
        keyval::Transaction* trans, const std::string& name, FileId id);
    void link(
        keyval::Transaction* trans, const std::string& name, ObjFile* file,
        bool saveMeta);
    void unlink(
        const Credential& cred, keyval::Transaction* trans,
        const std::string& name, ObjFile* file, bool saveMeta);
    std::shared_ptr<ObjFile> createNewFile(
        std::unique_lock<std::mutex>& lock,
        const Credential& cred,
        PosixType type,
        const std::string& name,
        std::function<void(Setattr*)> attrCb,
        std::function<void(
            keyval::Transaction*, std::shared_ptr<ObjFile>)> writeCb);
    void checkAccess(const Credential& cred, int accmode);
    void checkSticky(const Credential& cred, ObjFile* file);
    uint64_t getTime();
    void updateAccessTime();
    void updateModifyTime();
    virtual void truncate(
        const Credential& cred, keyval::Transaction* trans,
        std::uint64_t newSize);

protected:
    std::mutex mutex_;
    std::weak_ptr<ObjFilesystem> fs_;
    ObjFileMetaImpl meta_;
};

class ObjOpenFile: public OpenFile
{
public:
    ObjOpenFile(const Credential& cred, std::shared_ptr<ObjFile> file, int flags)
        : cred_(cred),
          file_(file),
          flags_(flags)
    {
    }

    ~ObjOpenFile() override;

    // OpenFile overrides
    std::shared_ptr<File> file() const override { return file_; }
    std::shared_ptr<Buffer> read(
        std::uint64_t offset, std::uint32_t size, bool& eof) override;
    std::uint32_t write(
        std::uint64_t offset, std::shared_ptr<Buffer> data) override;
    void flush() override;

private:
    Credential cred_;
    std::shared_ptr<ObjFile> file_;
    int flags_;
    bool needFlush_ = false;
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
    std::unique_ptr<keyval::Iterator> iterator_;
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
    ObjFilesystem(
        std::unique_ptr<keyval::Database> db,
        std::uint64_t blockSize = 4096);
    ObjFilesystem(
        std::unique_ptr<keyval::Database> db,
        std::shared_ptr<detail::Clock> clock,
        std::uint64_t blockSize = 4096);
    ~ObjFilesystem() override;

    std::shared_ptr<File> root() override;
    const FilesystemId& fsid() const override;
    std::shared_ptr<File> find(const FileHandle& fh) override;

    auto defaultNS() const { return defaultNS_; }
    auto directoriesNS() const { return directoriesNS_; }
    auto dataNS() const { return dataNS_; }

    auto clock() const { return clock_; }

    keyval::Database* db() const
    {
        return db_.get();
    }

    std::uint32_t blockSize() const
    {
        return blockSize_;
    }

    FileId nextId()
    {
        return FileId(nextId_++);
    }

    std::shared_ptr<ObjFile> find(FileId fileid);
    virtual std::shared_ptr<ObjFile> makeNewFile(FileId fileid);
    virtual std::shared_ptr<ObjFile> makeNewFile(ObjFileMetaImpl&& meta);
    virtual std::shared_ptr<OpenFile> makeNewOpenFile(
        const Credential& cred, std::shared_ptr<ObjFile> file, int flags);
    void remove(FileId fileid);
    void add(std::shared_ptr<ObjFile> file);
    void writeMeta(keyval::Transaction* trans);
    void setFsid();

protected:
    std::mutex mutex_;
    std::shared_ptr<detail::Clock> clock_;
    std::unique_ptr<keyval::Database> db_;
    std::shared_ptr<keyval::Namespace> defaultNS_;
    std::shared_ptr<keyval::Namespace> directoriesNS_;
    std::shared_ptr<keyval::Namespace> dataNS_;
    ObjFilesystemMeta meta_;
    std::uint32_t blockSize_;
    std::atomic<std::uint64_t> nextId_;
    FilesystemId fsid_;
    std::shared_ptr<ObjFile> root_;
    detail::LRUCache<std::uint64_t, ObjFile> cache_;
};

class ObjFilesystemFactory: public FilesystemFactory
{
public:
    std::string name() const override { return "objfs"; }
    std::shared_ptr<Filesystem> mount(const std::string& url) override;
};

void init(FilesystemManager* fsman);

}
}
