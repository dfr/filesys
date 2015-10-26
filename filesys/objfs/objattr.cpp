#include <cassert>
#include <chrono>
#include <system_error>

#include "objfs.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace std;
using namespace std::chrono;

FileType ObjGetattr::type() const
{
    switch (meta_.attr.type) {
    case PT_REG:
        return FileType::FILE;
    case PT_DIR:
        return FileType::DIRECTORY;
    case PT_BLK:
        return FileType::BLOCKDEV;
    case PT_CHR:
        return FileType::CHARDEV;
    case PT_LNK:
        return FileType::SYMLINK;
    case PT_SOCK:
        return FileType::SOCKET;
    case PT_FIFO:
        return FileType::FIFO;
    }
}

int ObjGetattr::mode() const
{
    return meta_.attr.mode;
}

int ObjGetattr::nlink() const
{
    return meta_.attr.nlink;
}

int ObjGetattr::uid() const
{
    return meta_.attr.uid;
}

int ObjGetattr::gid() const
{
    return meta_.attr.gid;
}

std::uint64_t ObjGetattr::size() const
{
    return meta_.attr.size;
}

std::uint64_t ObjGetattr::used() const
{
    return used_;
}

FileId ObjGetattr::fileid() const
{
    return FileId(meta_.fileid);
}

std::chrono::system_clock::time_point ObjGetattr::mtime() const
{
    return system_clock::time_point(
        duration_cast<system_clock::duration>(nanoseconds(meta_.attr.mtime)));
}

std::chrono::system_clock::time_point ObjGetattr::atime() const
{
    return system_clock::time_point(
        duration_cast<system_clock::duration>(nanoseconds(meta_.attr.atime)));
}

std::chrono::system_clock::time_point ObjGetattr::ctime() const
{
    return system_clock::time_point(
        duration_cast<system_clock::duration>(nanoseconds(meta_.attr.ctime)));
}

std::chrono::system_clock::time_point ObjGetattr::birthtime() const
{
    return system_clock::time_point(
        duration_cast<system_clock::duration>(
            nanoseconds(meta_.attr.birthtime)));
}

void ObjSetattr::setMode(int mode)
{
    meta_.attr.mode = mode;
}

void ObjSetattr::setUid(int uid)
{
    meta_.attr.uid = uid;
}

void ObjSetattr::setGid(int gid)
{
    meta_.attr.gid = gid;
}

void ObjSetattr::setSize(std::uint64_t size)
{
    meta_.attr.size = size;
}

void ObjSetattr::setMtime(std::chrono::system_clock::time_point mtime)
{
    meta_.attr.mtime =
        duration_cast<nanoseconds>(mtime.time_since_epoch()).count();
}

void ObjSetattr::setAtime(std::chrono::system_clock::time_point atime)
{
    meta_.attr.atime =
        duration_cast<nanoseconds>(atime.time_since_epoch()).count();
}
