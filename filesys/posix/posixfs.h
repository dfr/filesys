#pragma once

#include <list>
#include <unordered_map>
#include <dirent.h>

#include <sys/types.h>
#include <sys/mount.h>
#include <sys/stat.h>

#include <fs++/filesys.h>

namespace filesys {
namespace posix {

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
    std::uint64_t fileid() const override;
    std::chrono::system_clock::time_point mtime() const override;
    std::chrono::system_clock::time_point atime() const override;
    std::chrono::system_clock::time_point ctime() const override;

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
    void setMtime(std::chrono::system_clock::time_point mtime) {}
    void setAtime(std::chrono::system_clock::time_point atime) {}

    bool hasMode_ = false;
    bool hasSize_ = false;
    int mode_ = 0;
    std::uint64_t size_ = 0;
};

class PosixFsattr: public Fsattr
{
public:
    size_t tbytes() const override
    {
        return stat.f_blocks * stat.f_bsize;
    }
    size_t fbytes() const override
    {
        return stat.f_bfree * stat.f_bsize;
    }
    size_t abytes() const override
    {
        return stat.f_bavail * stat.f_bsize;
    }
    size_t tfiles() const override
    {
        return stat.f_files;
    }
    size_t ffiles() const override
    {
        return stat.f_ffree;
    }
    size_t afiles() const override
    {
        return stat.f_ffree;
    }

    struct ::statfs stat;
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
    std::shared_ptr<DirectoryIterator> readdir() override;
    std::shared_ptr<Fsattr> fsstat() override;

    std::uint64_t fileid() const { return id_; }
    int fd() const { return fd_; }

private:
    std::weak_ptr<PosixFilesystem> fs_;
    std::shared_ptr<PosixFile> parent_;
    std::string name_;
    std::uint64_t id_;
    int fd_;
};

class PosixDirectoryIterator: public DirectoryIterator
{
public:
    PosixDirectoryIterator(
        std::shared_ptr<PosixFilesystem> fs,
        std::shared_ptr<PosixFile> parent);
    ~PosixDirectoryIterator();

    bool valid() const override;
    std::uint64_t fileid() const override;
    std::string name() const override;
    std::shared_ptr<File> file() const override;
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

    std::shared_ptr<PosixFile> find(
        std::shared_ptr<PosixFile> parent,
        const std::string& name, int fd);
    std::shared_ptr<PosixFile> find(
        std::shared_ptr<PosixFile> parent,
        const std::string& name, std::uint64_t id, int fd);

private:
    int rootfd_;
    std::uint64_t rootid_;

    typedef std::list<std::shared_ptr<PosixFile>> lruT;
    lruT lru_;
    std::unordered_map<std::uint64_t, lruT::iterator> cache_;
    int maxCache_ = 1024;
};

class PosixFilesystemFactory: public FilesystemFactory
{
public:
    std::string name() const { return "file"; }
    std::pair<std::shared_ptr<Filesystem>, std::string> mount(
        const std::string& url) override;
};

void init(FilesystemManager* fsman);

}
}