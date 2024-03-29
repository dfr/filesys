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

#pragma once

#include <climits>
#include <string>
#include <map>
#include <vector>

#include <filesys/filesys.h>

namespace filesys {
namespace pfs {

constexpr int PFS_NAME_MAX = 128;

class PfsFilesystem;

class PfsGetattr: public Getattr
{
public:
    PfsGetattr(FileId fileid, std::chrono::system_clock::time_point time)
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
        return 0;
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

class PfsFsattr: public Fsattr
{
public:
    size_t totalSpace() const override { return 0; }
    size_t freeSpace() const override { return 0; }
    size_t availSpace() const override { return 0; }
    size_t totalFiles() const override { return 0; }
    size_t freeFiles() const override { return 0; }
    size_t availFiles() const override { return 0; }

    int linkMax() const override
    {
        return 0;
    }
    int nameMax() const override
    {
        return NAME_MAX;
    }
    int repairQueueSize() const override
    {
        return 0;
    }
};

class PfsFile: public File, public std::enable_shared_from_this<PfsFile>
{
public:
    PfsFile(std::shared_ptr<PfsFilesystem>,
            FileId fileid, std::shared_ptr<PfsFile> parent);

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

    FileId fileid() const { return fileid_; }
    std::shared_ptr<PfsFile> parent() const { return parent_; }
    std::shared_ptr<PfsFile> find(const std::string& name);
    void add(const std::string& name, std::shared_ptr<PfsFile> dir)
    {
        entries_[name] = dir;
    }
    void mount(std::shared_ptr<File> mount)
    {
        mount_ = mount;
    }
    std::shared_ptr<File> checkMount()
    {
        if (mount_)
            return mount_;
        else
            return shared_from_this();
    }

private:
    std::weak_ptr<PfsFilesystem> fs_;
    FileId fileid_;
    std::chrono::system_clock::time_point ctime_;
    std::shared_ptr<PfsFile> parent_;
    std::shared_ptr<File> mount_;
    std::map<std::string, std::weak_ptr<PfsFile>> entries_;
};

class PfsDirectoryIterator: public DirectoryIterator
{
public:
    PfsDirectoryIterator(
        const std::map<std::string, std::weak_ptr<PfsFile>>& entries);

    bool valid() const override;
    FileId fileid() const override;
    std::string name() const override;
    std::shared_ptr<File> file() const override;
    uint64_t seek() const override;
    void next() override;

private:
    void skipExpired();
    const std::map<std::string, std::weak_ptr<PfsFile>>& entries_;
    std::map<std::string, std::weak_ptr<PfsFile>>::const_iterator p_;
};

class PfsFilesystem: public Filesystem,
                     public std::enable_shared_from_this<PfsFilesystem>
{
public:
    PfsFilesystem();
    std::shared_ptr<File> root() override;
    const FilesystemId& fsid() const override;
    std::shared_ptr<File> find(const FileHandle& fh) override;

    /// Add a path to the filesystem
    void add(const std::string& path, std::shared_ptr<File> mount);
    void add(const std::string& path, std::shared_ptr<Filesystem> mount);

    /// Remove a path from the filesystem
    void remove(const std::string& path);

private:
    void checkRoot();

    static int nextfsid_;
    FilesystemId fsid_;
    int nextid_ = 1;
    std::shared_ptr<PfsFile> root_;
    std::map<int, std::weak_ptr<PfsFile>> idmap_;
    std::map<std::string, std::shared_ptr<PfsFile>> paths_;
    std::vector<std::shared_ptr<Filesystem>> subfs_;
};

}
}
