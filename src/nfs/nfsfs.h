#pragma once

#include <list>

#include <fs++/filesys.h>
#include <fs++/nfsfs.h>

#include "src/nfs/nfs_prot.h"

namespace filesys {
namespace nfs {

class NfsFilesystem;

class NfsGetattr: public Getattr
{
public:
    NfsGetattr(const fattr3& attr);

    // Getattr overrides
    FileType type() const override;
    int mode() const override;
    int nlink() const override;
    int uid() const override;
    int gid() const override;
    std::uint64_t size() const override;
    std::uint64_t used() const override;
    std::uint64_t fileid() const override;
    std::chrono::system_clock::time_point mtime() const override;
    std::chrono::system_clock::time_point atime() const override;
    std::chrono::system_clock::time_point ctime() const override;

private:
    fattr3 attr_;
};

class NfsSettattr: public Setattr
{
public:
    NfsSettattr(sattr3& attr) : attr_(attr) {}

    // Setattr overrides
    void setMode(int mode) override;
    void setUid(int uid) override;
    void setGid(int gid) override;
    void setSize(std::uint64_t size) override;
    void setMtime(std::chrono::system_clock::time_point mtime) override;
    void setAtime(std::chrono::system_clock::time_point atime) override;

    sattr3& attr_;
};

class NfsFile: public File, public std::enable_shared_from_this<NfsFile>
{
public:
    NfsFile(std::shared_ptr<NfsFilesystem> fs, nfs_fh3&& fh, fattr3&& attr);

    // File overrides
    std::shared_ptr<Getattr> getattr() override;
    void setattr(std::function<void(Setattr*)> cb) override;
    std::shared_ptr<File> lookup(const std::string& name) override;
    std::shared_ptr<File> open(
        const std::string& name, int flags,
        std::function<void(Setattr*)> cb) override;
    void close() override;
    void commit() override;
    std::string readlink() override;
    std::vector<std::uint8_t> read(
        std::uint64_t offset, std::uint32_t size, bool& eof) override;
    std::uint32_t write(
        std::uint64_t offset, const std::vector<std::uint8_t>& data) override;
    std::shared_ptr<File> mkdir(
        const std::string& name, std::function<void(Setattr*)> cb) override;
    std::shared_ptr<DirectoryIterator> readdir() override;

    std::uint64_t fileid() const { return attr_.fileid; }
    const nfs_fh3& fh() const { return fh_; }
    std::shared_ptr<NfsFilesystem> fs() const { return fs_.lock(); }
    void updateAttr(post_op_attr&& attr);

private:
    std::weak_ptr<NfsFilesystem> fs_;
    nfs_fh3 fh_;
    fattr3 attr_;
};

class NfsDirectoryIterator: public DirectoryIterator
{
public:
    NfsDirectoryIterator(std::shared_ptr<NfsFile> dir);

    bool valid() const override;
    std::uint64_t fileid() const override;
    std::string name() const override;
    std::shared_ptr<File> file() const override;
    void next() override;

private:
    void readdir(cookie3 cookie);

    std::shared_ptr<NfsFile> dir_;
    mutable std::shared_ptr<File> file_;
    cookieverf3 verf_;
    std::unique_ptr<entryplus3> entry_;
    bool eof_;
};

class NfsFilesystem: public Filesystem,
                     public std::enable_shared_from_this<NfsFilesystem>,
                     public NfsProgram3<oncrpc::SysClient>
{
public:
    NfsFilesystem(std::shared_ptr<oncrpc::Channel> chan, nfs_fh3&& rootfh);
    std::shared_ptr<File> root() override;

    std::shared_ptr<NfsFile> find(nfs_fh3&& fh);
    std::shared_ptr<NfsFile> find(nfs_fh3&& fh, fattr3&& attr);
private:

    std::shared_ptr<oncrpc::Channel> channel_;
    std::shared_ptr<oncrpc::Client> client_;
    nfs_fh3 rootfh_;
    typedef std::list<std::shared_ptr<NfsFile>> lruT;
    lruT lru_;
    std::unordered_map<std::uint64_t, lruT::iterator> cache_;
    int maxCache_ = 1024;
};

}
}
