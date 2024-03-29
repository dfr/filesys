/*-
 * Copyright (c) 2016-present Doug Rabson
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

// -*- c++ -*-
#pragma once

#include <dirent.h>

#include <sys/types.h>
#include <sys/mount.h>
#include <sys/stat.h>

#include <filesys/filesys.h>
#include <util/lrucache.h>

namespace filesys {
namespace posix {

class PosixFile;
class PosixFilesystem;

class PosixGetattr: public Getattr
{
public:
    PosixGetattr(const struct ::stat& st);

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
    struct ::stat stat_;
};

class PosixSetattr: public Setattr
{
public:
    // Setattr overrides
    void setMode(int mode) override
    {
        hasMode_ = true;
        mode_ = mode;
    }
    void setUid(int uid) override {}
    void setGid(int gid) override {}
    void setSize(std::uint64_t size) override
    {
        hasSize_ = true;
        size_ = size;
    }
    void setMtime(std::chrono::system_clock::time_point mtime) override {}
    void setAtime(std::chrono::system_clock::time_point atime) override {}
    void setChange(std::uint64_t change) override {}
    void setCreateverf(std::uint64_t verf) override {}

    bool hasMode_ = false;
    bool hasSize_ = false;
    int mode_ = 0;
    std::uint64_t size_ = 0;
};

class PosixFsattr: public Fsattr
{
public:
    size_t totalSpace() const override
    {
        return stat.f_blocks * stat.f_bsize;
    }
    size_t freeSpace() const override
    {
        return stat.f_bfree * stat.f_bsize;
    }
    size_t availSpace() const override
    {
        if (privcred_)
            return stat.f_bfree * stat.f_bsize;
        else
            return stat.f_bavail * stat.f_bsize;
    }

    size_t totalFiles() const override
    {
        return stat.f_files;
    }
    size_t freeFiles() const override
    {
        return stat.f_ffree;
    }
    size_t availFiles() const override
    {
        return stat.f_ffree;
    }

    int linkMax() const override
    {
        return linkMax_;
    }
    int nameMax() const override
    {
        return nameMax_;
    }
    int repairQueueSize() const override
    {
        return 0;
    }

    struct ::statfs stat;
    int linkMax_;
    int nameMax_;
    bool privcred_;
};

class PosixFile: public File, public std::enable_shared_from_this<PosixFile>
{
public:
    PosixFile(
        std::shared_ptr<PosixFilesystem> fs,
        std::shared_ptr<PosixFile> parent,
        const std::string& name, std::uint64_t id, int fd);
    ~PosixFile();

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

    // LRUCache compliance
    int cost() const { return 1; }

    FileId fileid() const { return id_; }
    int fd() const { return fd_; }

private:
    std::weak_ptr<PosixFilesystem> fs_;
    std::shared_ptr<PosixFile> parent_;
    std::string name_;
    FileId id_;
    int fd_;
};

class PosixOpenFile: public OpenFile
{
public:
    PosixOpenFile(std::shared_ptr<PosixFile> file)
        : file_(file)
    {
    }

    // OpenFile overrides
    std::shared_ptr<File> file() const override { return file_; }
    std::shared_ptr<Buffer> read(
        std::uint64_t offset, std::uint32_t size, bool& eof) override;
    std::uint32_t write(
        std::uint64_t offset, std::shared_ptr<Buffer> data) override;
    void flush() override;

private:
    std::shared_ptr<PosixFile> file_;
};

class PosixDirectoryIterator: public DirectoryIterator
{
public:
    PosixDirectoryIterator(
        std::shared_ptr<PosixFilesystem> fs,
        std::shared_ptr<PosixFile> parent);
    ~PosixDirectoryIterator();

    bool valid() const override;
    FileId fileid() const override;
    std::string name() const override;
    std::shared_ptr<File> file() const override;
    uint64_t seek() const override;
    void next() override;

private:
    std::shared_ptr<PosixFilesystem> fs_;
    std::shared_ptr<PosixFile> parent_;
    DIR* dir_;
    dirent* next_;
};

class PosixFilesystem: public Filesystem,
                       public std::enable_shared_from_this<PosixFilesystem>
{
public:
    PosixFilesystem(const std::string& path);
    std::shared_ptr<File> root() override;
    const FilesystemId& fsid() const override;
    std::shared_ptr<File> find(const FileHandle& fh) override;

    std::shared_ptr<PosixFile> find(
        std::shared_ptr<PosixFile> parent,
        const std::string& name, int fd);
    std::shared_ptr<PosixFile> find(
        std::shared_ptr<PosixFile> parent,
        const std::string& name, FileId id, int fd);
    void remove(FileId id);

private:
    int rootfd_;
    FileId rootid_;
    FilesystemId fsid_;
    util::LRUCache<std::uint64_t, PosixFile> cache_;
};

class PosixFilesystemFactory: public FilesystemFactory
{
public:
    std::string name() const override { return "file"; }
    std::shared_ptr<Filesystem> mount(const std::string& url) override;
};

void init(FilesystemManager* fsman);

}
}
