#pragma once

#include <list>

#include <fs++/filesys.h>
#include <fs++/proto/nfs_prot.h>

namespace filesys {
namespace nfs {

constexpr detail::Clock::duration ATTR_TIMEOUT = std::chrono::seconds(5);

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
    FileId fileid() const override;
    std::chrono::system_clock::time_point mtime() const override;
    std::chrono::system_clock::time_point atime() const override;
    std::chrono::system_clock::time_point ctime() const override;
    std::chrono::system_clock::time_point birthtime() const override;

private:
    fattr3 attr_;
};

class NfsSetattr: public Setattr
{
public:
    NfsSetattr(sattr3& attr);

    // Setattr overrides
    void setMode(int mode) override;
    void setUid(int uid) override;
    void setGid(int gid) override;
    void setSize(std::uint64_t size) override;
    void setMtime(std::chrono::system_clock::time_point mtime) override;
    void setAtime(std::chrono::system_clock::time_point atime) override;

private:
    sattr3& attr_;
};

class NfsFsattr: public Fsattr
{
public:
    NfsFsattr(const FSSTAT3resok& stat, const PATHCONF3resok& pc);

    size_t tbytes() const override { return tbytes_; }
    size_t fbytes() const override { return fbytes_; }
    size_t abytes() const override { return abytes_; }
    size_t tfiles() const override { return tfiles_; }
    size_t ffiles() const override { return ffiles_; }
    size_t afiles() const override { return afiles_; }
    int linkMax() const override
    {
        return linkMax_;
    }
    int nameMax() const override
    {
        return nameMax_;
    }

private:
    size_t tbytes_;
    size_t fbytes_;
    size_t abytes_;
    size_t tfiles_;
    size_t ffiles_;
    size_t afiles_;
    int linkMax_;
    int nameMax_;
};

class NfsFile: public File, public std::enable_shared_from_this<NfsFile>
{
public:
    NfsFile(std::shared_ptr<NfsFilesystem> fs, nfs_fh3&& fh, fattr3&& attr);

    // File overrides
    std::shared_ptr<Filesystem> fs() override;
    void handle(FileHandle& fh) override;
    std::shared_ptr<Getattr> getattr() override;
    void setattr(std::function<void(Setattr*)> cb) override;
    std::shared_ptr<File> lookup(const std::string& name) override;
    std::shared_ptr<File> open(
        const std::string& name, int flags,
        std::function<void(Setattr*)> cb) override;
    void close() override;
    void commit() override;
    std::string readlink() override;
    std::shared_ptr<oncrpc::Buffer> read(
        std::uint64_t offset, std::uint32_t size, bool& eof) override;
    std::uint32_t write(
        std::uint64_t offset, std::shared_ptr<oncrpc::Buffer> data) override;
    std::shared_ptr<File> mkdir(
        const std::string& name, std::function<void(Setattr*)> cb) override;
    std::shared_ptr<File> symlink(
        const std::string& name, const std::string& data,
        std::function<void(Setattr*)> cb) override;
    std::shared_ptr<File> mkfifo(
        const std::string& name, std::function<void(Setattr*)> cb) override;
    void remove(const std::string& name) override;
    void rmdir(const std::string& name) override;
    void rename(
        const std::string& toName,
        std::shared_ptr<File> fromDir,
        const std::string& fromName) override;
    void link(const std::string& name, std::shared_ptr<File> file) override;
    std::shared_ptr<DirectoryIterator> readdir(std::uint64_t seek) override;
    std::shared_ptr<Fsattr> fsstat() override;

    FileId fileid() const { return FileId(attr_.fileid); }
    const nfs_fh3& fh() const { return fh_; }
    std::shared_ptr<NfsFilesystem> nfs() const { return fs_.lock(); }
    std::shared_ptr<File> find(
        const std::string& name, post_op_fh3& fh, post_op_attr& attr);
    void update(post_op_attr&& attr);
    void update(fattr3&& attr);

private:
    std::weak_ptr<NfsFilesystem> fs_;
    nfs_fh3 fh_;
    detail::Clock::time_point attrTime_;
    fattr3 attr_;
};

class NfsDirectoryIterator: public DirectoryIterator
{
public:
    NfsDirectoryIterator(std::shared_ptr<NfsFile> dir, std::uint64_t seek);

    bool valid() const override;
    FileId fileid() const override;
    std::string name() const override;
    std::shared_ptr<File> file() const override;
    uint64_t seek() const override;
    void next() override;

private:
    void readdir(cookie3 cookie);

    std::shared_ptr<NfsFile> dir_;
    mutable std::shared_ptr<File> file_;
    cookieverf3 verf_;
    std::unique_ptr<entryplus3> entry_;
    bool eof_;
};

struct NfsFsinfo
{
    std::uint32_t rtmax;
    std::uint32_t rtpref;
    std::uint32_t rtmult;
    std::uint32_t wtmax;
    std::uint32_t wtpref;
    std::uint32_t wtmult;
    std::uint32_t dtpref;
    std::uint64_t maxfilesize;
    nfstime3 timedelta;
    std::uint32_t properties;
};

class NfsFilesystem: public Filesystem,
                     public std::enable_shared_from_this<NfsFilesystem>
{
public:
    NfsFilesystem(
        std::shared_ptr<INfsProgram3> proto,
        std::shared_ptr<detail::Clock> clock,
        nfs_fh3&& rootfh);
    ~NfsFilesystem();
    std::shared_ptr<File> root() override;
    const FilesystemId& fsid() const override;
    std::shared_ptr<File> find(const FileHandle& fh) override;

    auto proto() const { return proto_; }
    auto clock() const { return clock_; }
    std::shared_ptr<NfsFile> find(nfs_fh3&& fh);
    std::shared_ptr<NfsFile> find(nfs_fh3&& fh, fattr3&& attrp);
    const NfsFsinfo& fsinfo() const { return fsinfo_; }

private:
    std::shared_ptr<INfsProgram3> proto_;
    std::shared_ptr<detail::Clock> clock_;
    nfs_fh3 rootfh_;
    std::shared_ptr<File> root_;
    NfsFsinfo fsinfo_;
    typedef std::list<std::shared_ptr<NfsFile>> lruT;
    lruT lru_;
    std::unordered_map<std::uint64_t, lruT::iterator> cache_;
    static constexpr int maxCache_ = 1024;
};

class NfsFilesystemFactory: public FilesystemFactory
{
public:
    std::string name() const override { return "nfs"; }
    std::pair<std::shared_ptr<Filesystem>, std::string> mount(
        FilesystemManager* fsman, const std::string& url) override;
};

void init(FilesystemManager* fsman);

}
}
