#pragma once

#include <string>
#include <map>
#include <vector>

#include <fs++/filesys.h>

namespace filesys {
namespace pfs {

class PfsFilesystem;

class PfsGetattr: public Getattr
{
public:
    PfsGetattr(int fileid, std::chrono::system_clock::time_point time)
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
    std::uint64_t fileid() const override
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

private:
    int fileid_;
    std::chrono::system_clock::time_point time_;
};

class PfsFsattr: public Fsattr
{
public:
    size_t tbytes() const override { return 0; }
    size_t fbytes() const override { return 0; }
    size_t abytes() const override { return 0; }
    size_t tfiles() const override { return 0; }
    size_t ffiles() const override { return 0; }
    size_t afiles() const override { return 0; }
};

class PfsFile: public File, public std::enable_shared_from_this<PfsFile>
{
public:
    PfsFile(std::shared_ptr<PfsFilesystem>,
            int fileid, std::shared_ptr<PfsFile> parent);

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
    std::shared_ptr<DirectoryIterator> readdir() override;
    std::shared_ptr<Fsattr> fsstat() override;

    int fileid() const { return fileid_; }
    std::shared_ptr<PfsFile> parent() const { return parent_; }
    std::shared_ptr<PfsFile> find(const std::string& name);
    void add(const std::string& name, std::shared_ptr<PfsFile> dir)
    {
        entries_[name] = dir;
    }
    void mount(std::shared_ptr<Filesystem> mount)
    {
        mount_ = mount;
    }
    std::shared_ptr<File> checkMount()
    {
        if (mount_)
            return mount_->root();
        else
            return shared_from_this();
    }

private:
    std::weak_ptr<PfsFilesystem> fs_;
    int fileid_;
    std::chrono::system_clock::time_point ctime_;
    std::shared_ptr<PfsFile> parent_;
    std::shared_ptr<Filesystem> mount_;
    std::map<std::string, std::weak_ptr<PfsFile>> entries_;
};

class PfsDirectoryIterator: public DirectoryIterator
{
public:
    PfsDirectoryIterator(
        const std::map<std::string, std::weak_ptr<PfsFile>>& entries);

    bool valid() const override;
    std::uint64_t fileid() const override;
    std::string name() const override;
    std::shared_ptr<File> file() const override;
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

    /// Add a path to the filesystem
    void add(const std::string& path,
             std::shared_ptr<Filesystem> mount = nullptr);

    /// Remove a path from the filesystem
    void remove(const std::string& path);

private:
    int nextid_ = 1;
    std::shared_ptr<PfsFile> root_;
    std::map<std::string, std::shared_ptr<PfsFile>> paths_;
};

}
}
