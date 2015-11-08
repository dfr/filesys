#include <cassert>
#include <fcntl.h>
#include <system_error>
#include <unistd.h>

#include <glog/logging.h>

#include "posixfs.h"

#ifdef __APPLE__

// XXX not thread safe due to the abuse of chdir.
int mkfifoat(int fd, const char* path, int mode)
{
    ::fchdir(fd);
    int res = ::mkfifo(path, mode);
    ::chdir(::getenv("PWD"));
    return res;
}

#endif

using namespace filesys;
using namespace filesys::posix;
using namespace std;

PosixFile::PosixFile(
    shared_ptr<PosixFilesystem> fs, shared_ptr<PosixFile> parent,
    const string& name, uint64_t fileid, int fd)
    : fs_(fs),
      parent_(parent),
      name_(name),
      id_(fileid),
      fd_(fd)
{
}

PosixFile::~PosixFile()
{
    ::close(fd_);
}

shared_ptr<Filesystem> PosixFile::fs()
{
    return fs_.lock();
}

void
PosixFile::handle(FileHandle& fh)
{
    fh.fsid = fs_.lock()->fsid();
    fh.handle.resize(sizeof(FileId));
    *reinterpret_cast<FileId*>(fh.handle.data()) = id_;
}

shared_ptr<Getattr> PosixFile::getattr()
{
    struct ::stat st;
    if (fd_ == -1) {
        // We don't have open file descriptors to symbolic links
        if (::fstatat(
            parent_->fd_, name_.c_str(), &st, AT_SYMLINK_NOFOLLOW) < 0)
            throw system_error(errno, system_category());
        if (id_ != st.st_ino) {
            LOG(ERROR) << "symbolic link renamed?";
            throw system_error(EEXIST, system_category());
        }
    }
    else {
        if (::fstat(fd_, &st) < 0)
            throw system_error(errno, system_category());
    }
    return make_shared<PosixGetattr>(st);
}

void PosixFile::setattr(function<void(Setattr*)> cb)
{
    PosixSetattr attr;
    cb(&attr);
    if (attr.hasMode_) {
        if (::fchmod(fd_, attr.mode_) < 0)
            throw system_error(errno, system_category());
    }
    if (attr.hasSize_) {
        if (::ftruncate(fd_, attr.size_) < 0)
            throw system_error(errno, system_category());
    }
}

shared_ptr<File> PosixFile::lookup(const string& name)
{
    // Don't allow the user to escape the root directory
    if (name == "..") {
        if (this == fs_.lock()->root().get())
            return shared_from_this();
    }
    if (name[0] == '/')
        throw system_error(EACCES, system_category());

    int fd;
    int oflag;
    if (::faccessat(fd_, name.c_str(), W_OK, 0) >= 0)
        oflag = O_RDWR;
    else
        oflag = O_RDONLY;
#ifdef O_SYMLINK
    oflag |= O_SYMLINK;
#endif
    fd = ::openat(fd_, name.c_str(), oflag);
    if (fd < 0 && errno == EISDIR) {
        fd = ::openat(fd_, name.c_str(), O_RDONLY);
    }
    if (fd < 0)
        throw system_error(errno, system_category());
    return fs_.lock()->find(shared_from_this(), name, fd);
}

shared_ptr<File> PosixFile::open(
    const string& name, int flags, function<void(Setattr*)> cb)
{
    // Don't allow the user to escape the root directory
    if (name == "..") {
        if (this == fs_.lock()->root().get())
            return shared_from_this();
    }
    if (name[0] == '/')
        throw system_error(EACCES, system_category());

    int fd;
    int oflag;
    if (::faccessat(fd_, name.c_str(), W_OK, 0))
        oflag = O_RDWR;
    else
        oflag = O_RDONLY;
    if (flags & OpenFlags::CREATE)
        oflag |= O_CREAT;
    if (flags & OpenFlags::TRUNCATE)
        oflag |= O_TRUNC;
    if (flags & OpenFlags::EXCLUSIVE)
        oflag |= O_EXCL;
    PosixSetattr attr;
    cb(&attr);
    int mode = attr.hasMode_ ? attr.mode_ : 0;
    fd = ::openat(fd_, name.c_str(), oflag, mode);
    if (fd < 0)
        throw system_error(errno, system_category());
    return fs_.lock()->find(shared_from_this(), name, fd);

    throw system_error(EISDIR, system_category());
}

void PosixFile::close()
{
}

void PosixFile::commit()
{
}

string PosixFile::readlink()
{
    // XXX: really want freadlink here
    char buf[PATH_MAX];
    auto n = ::readlinkat(parent_->fd_, name_.c_str(), buf, sizeof(buf) - 1);
    if (n < 0)
        throw system_error(errno, system_category());
    buf[n] = '\0';
    return buf;
}

shared_ptr<oncrpc::Buffer>
PosixFile::read(uint64_t offset, uint32_t count, bool& eof)
{
    auto buf = make_shared<oncrpc::Buffer>(count);
    auto n = ::pread(fd_, buf->data(), count, offset);
    if (n < 0)
        throw system_error(errno, system_category());
    eof = n < count;
    if (n != count) {
        // Return a subset of the buffer we allocated
        buf = make_shared<oncrpc::Buffer>(buf, 0, n);
    }
    return buf;
}

uint32_t PosixFile::write(uint64_t offset, shared_ptr<oncrpc::Buffer> data)
{
    auto p = data->data();
    auto len = data->size();
    while (len > 0) {
        auto n = ::pwrite(fd_, p, len, offset);
        if (n < 0)
            throw system_error(errno, system_category());
        p += n;
        len -= n;
        offset += n;
    }
    return data->size();
}

shared_ptr<File> PosixFile::mkdir(
    const string& name, function<void(Setattr*)> cb)
{
    if (name[0] == '/')
        throw system_error(EACCES, system_category());
    PosixSetattr attr;
    cb(&attr);
    int mode = attr.hasMode_ ? attr.mode_ : 0;
    if (::mkdirat(fd_, name.c_str(), mode) >= 0)
        return lookup(name);
    throw system_error(errno, system_category());
}

shared_ptr<File> PosixFile::symlink(
    const string& name, const string& data,
    function<void(Setattr*)> cb)
{
    if (name[0] == '/')
        throw system_error(EACCES, system_category());
    if (::symlinkat(data.c_str(), fd_, name.c_str()) >= 0)
        return lookup(name);
    throw system_error(errno, system_category());
}

shared_ptr<File> PosixFile::mkfifo(
    const string& name, function<void(Setattr*)> cb)
{
    if (name[0] == '/')
        throw system_error(EACCES, system_category());
    PosixSetattr attr;
    cb(&attr);
    int mode = attr.hasMode_ ? attr.mode_ : 0;
    if (::mkfifoat(fd_, name.c_str(), mode) >= 0)
        return lookup(name);
    throw system_error(errno, system_category());
}

void PosixFile::remove(const string& name)
{
    if (name[0] == '/')
        throw system_error(EACCES, system_category());
    if (::unlinkat(fd_, name.c_str(), 0) < 0)
        throw system_error(errno, system_category());
}

void PosixFile::rmdir(const string& name)
{
    if (name[0] == '/')
        throw system_error(EACCES, system_category());
    if (::unlinkat(fd_, name.c_str(), AT_REMOVEDIR) < 0)
        throw system_error(errno, system_category());
}

void PosixFile::rename(
    const string& toName, shared_ptr<File> fromDir, const string& fromName)
{
    if (fromName[0] == '/' || toName[0] == '/')
        throw system_error(EACCES, system_category());
    auto from = dynamic_cast<PosixFile*>(fromDir.get());
    if (::renameat(from->fd_, fromName.c_str(), fd_, toName.c_str()) < 0)
        throw system_error(errno, system_category());
}

void PosixFile::link(const string& name, shared_ptr<File> file)
{
    if (name[0] == '/')
        throw system_error(EACCES, system_category());
    auto f = dynamic_cast<PosixFile*>(file.get());
    // XXX check to ensure f->name_ still refers to the same object
    if (::linkat(f->parent_->fd_, f->name_.c_str(), fd_, name.c_str(), 0) < 0)
        throw system_error(errno, system_category());
}

shared_ptr<DirectoryIterator> PosixFile::readdir(uint64_t seek)
{
    return make_shared<PosixDirectoryIterator>(fs_.lock(), shared_from_this());
}

shared_ptr<Fsattr> PosixFile::fsstat()
{
    auto res = make_shared<PosixFsattr>();
    if (::fstatfs(fd_, &res->stat) < 0)
        throw system_error(errno, system_category());
    return res;
}
