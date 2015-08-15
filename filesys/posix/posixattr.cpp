#include <fs++/filesys.h>
#include <glog/logging.h>

#include "posixfs.h"

using namespace filesys;
using namespace filesys::posix;
using namespace std::chrono;

static auto fromTimespec(const timespec& t)
{
    auto d = seconds(t.tv_sec) + nanoseconds(t.tv_nsec);
    return system_clock::time_point(duration_cast<system_clock::duration>(d));
}

PosixGetattr::PosixGetattr(const struct ::stat& st)
    : stat_(st)
{
}

FileType PosixGetattr::type() const
{
    if (S_ISREG(stat_.st_mode))
        return FileType::FILE;
    if (S_ISDIR(stat_.st_mode))
        return FileType::DIRECTORY;
    if (S_ISBLK(stat_.st_mode))
        return FileType::BLOCKDEV;
    if (S_ISCHR(stat_.st_mode))
        return FileType::CHARDEV;
    if (S_ISLNK(stat_.st_mode))
        return FileType::SYMLINK;
    if (S_ISSOCK(stat_.st_mode))
        return FileType::SOCKET;
    if (S_ISFIFO(stat_.st_mode))
        return FileType::FIFO;
    return FileType::BAD;
}

int PosixGetattr::mode() const
{
    return stat_.st_mode & 0777;
}

int PosixGetattr::nlink() const
{
    return stat_.st_nlink;
}

int PosixGetattr::uid() const
{
    return stat_.st_uid;
}

int PosixGetattr::gid() const
{
    return stat_.st_gid;
}

std::uint64_t PosixGetattr::size() const
{
    return stat_.st_size;
}

std::uint64_t PosixGetattr::used() const
{
    return stat_.st_blocks * stat_.st_blksize;
}

std::uint64_t PosixGetattr::fileid() const
{
    return stat_.st_ino;
}

std::chrono::system_clock::time_point PosixGetattr::mtime() const
{
    return fromTimespec(stat_.st_mtimespec);
}

std::chrono::system_clock::time_point PosixGetattr::atime() const
{
    return fromTimespec(stat_.st_atimespec);
}

std::chrono::system_clock::time_point PosixGetattr::ctime() const
{
    return fromTimespec(stat_.st_ctimespec);
}