#pragma once

#include <fs++/filecache.h>
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
    std::uint64_t createverf() const override;

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
    void setCreateverf(std::uint64_t verf) override;

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
    NfsFile(
        std::shared_ptr<NfsFilesystem> fs,
        const nfs_fh3& fh, const fattr3& attr);

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

    const FileId fileid() const { return FileId(attr_.fileid); }
    const nfs_fh3& fh() const { return fh_; }
    std::shared_ptr<NfsFilesystem> nfs() const { return fs_.lock(); }
    std::shared_ptr<File> find(
        const std::string& name,
        const post_op_fh3& fh, const post_op_attr& attr);
    void update(const post_op_attr& attr);
    void update(const fattr3& attr);

private:
    std::weak_ptr<NfsFilesystem> fs_;
    nfs_fh3 fh_;
    detail::Clock::time_point attrTime_;
    fattr3 attr_;
};

class NfsDirectoryIterator: public DirectoryIterator
{
public:
    NfsDirectoryIterator(
        const Credential& cred, std::shared_ptr<NfsFile> dir,
        std::uint64_t seek);

    bool valid() const override;
    FileId fileid() const override;
    std::string name() const override;
    std::shared_ptr<File> file() const override;
    uint64_t seek() const override;
    void next() override;

private:
    void readdir(cookie3 cookie);

    const Credential& cred_;
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

struct NfsFhHash
{
    size_t operator()(const filesys::nfs::nfs_fh3& fh) const
    {
        // This uses the djb2 hash
        size_t hash = 5381;
        for (auto c: fh.data)
            hash = (hash << 5) + hash + c; /* hash * 33 + c */
        return hash;
    }
};

static inline int operator==(const nfs_fh3& fh1, const nfs_fh3& fh2)
{
    return fh1.data == fh2.data;
}

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
    std::shared_ptr<NfsFile> find(const nfs_fh3& fh);
    std::shared_ptr<NfsFile> find(const nfs_fh3& fh, const fattr3& attrp);
    const NfsFsinfo& fsinfo() const { return fsinfo_; }

private:
    std::shared_ptr<INfsProgram3> proto_;
    std::shared_ptr<detail::Clock> clock_;
    nfs_fh3 rootfh_;
    std::shared_ptr<File> root_;
    NfsFsinfo fsinfo_;
    detail::FileCache<nfs_fh3, NfsFile, NfsFhHash> cache_;
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
