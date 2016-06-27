/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include <string>
#include <iostream>
#include <map>
#include <vector>

#include <fs++/filesys.h>
#include <fs++/lrucache.h>

namespace filesys {
namespace data {

static inline std::ostream& operator<<(std::ostream& os, const PieceId& id)
{
    os << "{" << id.fileid << "," << id.offset << "," << id.size << "}";
    return os;
}

struct PieceIdHash
{
    size_t operator()(const PieceId& id) const
    {
        std::hash<uint64_t> h;
        return h(id.fileid) + h(id.offset) + h(id.size);
    }
};

class DataFilesystem;

class DataGetattr: public Getattr
{
public:
    DataGetattr(FileId fileid, std::chrono::system_clock::time_point time)
        : fileid_(fileid),
          time_(time)
    {
    }

    // Getattr overrides
    FileType type() const override
    {
        return FileType::DIRECTORY;
    }
    int mode() const override
    {
        return 0555;
    }
    int nlink() const override
    {
        return 1;
    }
    int uid() const override
    {
        return 0;
    }
    int gid() const override
    {
        return 0;
    }
    std::uint64_t size() const override
    {
        return 0;
    }
    std::uint64_t used() const override
    {
        return 0;
    }
    std::uint32_t blockSize() const override
    {
        return 4096;
    }
    FileId fileid() const override
    {
        return fileid_;
    }
    std::chrono::system_clock::time_point mtime() const override
    {
        return time_;
    }
    std::chrono::system_clock::time_point atime() const override
    {
        return time_;
    }
    std::chrono::system_clock::time_point ctime() const override
    {
        return time_;
    }
    std::chrono::system_clock::time_point birthtime() const override
    {
        return time_;
    }
    std::uint64_t change() const override
    {
        return 1;
    }
    std::uint64_t createverf() const override
    {
        return 0;
    }

private:
    FileId fileid_;
    std::chrono::system_clock::time_point time_;
};

class DataFile: public File, public std::enable_shared_from_this<DataFile>
{
public:
    DataFile(
        std::shared_ptr<DataFilesystem>, PieceId id,
        std::shared_ptr<File> file);

    // File overrides
    std::shared_ptr<Filesystem> fs() override;
    FileHandle handle() override;
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

    // Return a File object from the backing store filesystem for this piece
    std::shared_ptr<File> backingFile();

private:
    std::weak_ptr<DataFilesystem> fs_;
    PieceId id_;
    std::weak_ptr<File> file_;
};

class DataRootGetattr: public Getattr
{
public:
    DataRootGetattr(std::shared_ptr<Filesystem> store)
        : storeAttr_(store->root()->getattr())
    {
    }

    // Getattr overrides
    FileType type() const override
    {
        return FileType::DIRECTORY;
    }
    int mode() const override
    {
        return 0555;
    }
    int nlink() const override
    {
        return 1;
    }
    int uid() const override
    {
        return 0;
    }
    int gid() const override
    {
        return 0;
    }
    std::uint64_t size() const override
    {
        return 0;
    }
    std::uint64_t used() const override
    {
        return 0;
    }
    std::uint32_t blockSize() const override
    {
        return 4096;
    }
    FileId fileid() const override
    {
        return FileId(1);
    }
    std::chrono::system_clock::time_point mtime() const override
    {
        return storeAttr_->mtime();
    }
    std::chrono::system_clock::time_point atime() const override
    {
        return storeAttr_->atime();
    }
    std::chrono::system_clock::time_point ctime() const override
    {
        return storeAttr_->ctime();
    }
    std::chrono::system_clock::time_point birthtime() const override
    {
        return storeAttr_->birthtime();
    }
    std::uint64_t change() const override
    {
        return storeAttr_->change();
    }
    std::uint64_t createverf() const override
    {
        return 0;
    }

private:
    std::shared_ptr<Getattr> storeAttr_;
};

class DataRoot: public File, public std::enable_shared_from_this<DataRoot>
{
public:
    DataRoot(std::shared_ptr<DataFilesystem>);

    // File overrides
    std::shared_ptr<Filesystem> fs() override;
    FileHandle handle() override;
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

private:
    std::weak_ptr<DataFilesystem> fs_;
};

class DataDirectoryIterator: public DirectoryIterator
{
public:
    DataDirectoryIterator(
        const Credential& cred, std::shared_ptr<Filesystem> store, int seek);

    bool valid() const override;
    FileId fileid() const override;
    std::string name() const override;
    std::shared_ptr<File> file() const override;
    uint64_t seek() const override;
    void next() override;

private:
    Credential cred_;

    // We iterate over the three directory levels for the piece store
    int seek_;
    int level_;
    std::shared_ptr<File> dirs_[4];
    std::shared_ptr<DirectoryIterator> iters_[4];
    bool valid_;
};

class DataFilesystem: public DataStore,
                      public std::enable_shared_from_this<DataFilesystem>
{
public:
    DataFilesystem(std::shared_ptr<Filesystem> store);

    // Filesystem overrides
    std::shared_ptr<File> root() override;
    const FilesystemId& fsid() const override;
    std::shared_ptr<File> find(const FileHandle& fh) override;
    bool isData() const override { return true; }

    // DataStore overrides
    std::shared_ptr<File> findPiece(
        const Credential& cred, const PieceId& id) override;
    std::shared_ptr<File> createPiece(
        const Credential& cred, const PieceId& id) override;
    void removePiece(
        const Credential& cred, const PieceId& id) override;

    auto store() const { return store_; }

    /// Return a FileHandle for this piece
    FileHandle pieceHandle(const PieceId& id);

    /// Return true if this data store has a copy of the given piece
    bool exists(const PieceId& id);

    // Lookup a DataFile object for the given piece id
    std::shared_ptr<DataFile> find(
        const Credential& cred, const PieceId& id);

    /// Return the underlying data file from the backing store for a
    /// particular piece, if it exists
    std::shared_ptr<File> lookup(
        const Credential& cred, const PieceId& id);

    /// Open the underlying data file from the backing store for a
    /// particular piece
    std::shared_ptr<OpenFile> open(
        const Credential& cred, const PieceId& id, int flags);

private:

    std::mutex mutex_;
    FilesystemId fsid_;
    std::shared_ptr<Filesystem> store_;
    std::shared_ptr<DataRoot> root_;
    detail::LRUCache<PieceId, DataFile, PieceIdHash> cache_;
};

class DataFilesystemFactory: public FilesystemFactory
{
public:
    std::string name() const override { return "datafs"; }
    std::shared_ptr<Filesystem> mount(const std::string& url) override;
};

void init(FilesystemManager* fsman);

}
}
