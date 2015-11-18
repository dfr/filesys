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
    if (cred_.uid() != meta_.attr.uid && !cred_.privileged())
        throw system_error(EPERM, system_category());
    meta_.attr.mode = mode;
}

void ObjSetattr::setUid(int uid)
{
    if (uid != meta_.attr.uid && !cred_.privileged())
        throw system_error(EPERM, system_category());
    meta_.attr.uid = uid;
}

void ObjSetattr::setGid(int gid)
{
    // If the cred matches the file owner and contains the requested group,
    // or the cred is privileged, we can change the file group
    if ((cred_.uid() == meta_.attr.uid && cred_.hasgroup(gid))
        || cred_.privileged())
        meta_.attr.gid = gid;
    else
        throw system_error(EPERM, system_category());
}

void ObjSetattr::setSize(std::uint64_t size)
{
    CheckAccess(
        meta_.attr.uid, meta_.attr.gid, meta_.attr.mode,
        cred_, AccessFlags::WRITE);
    meta_.attr.size = size;
}

void ObjSetattr::setMtime(std::chrono::system_clock::time_point mtime)
{
    // The file owner can change the times unconditionally
    if (meta_.attr.uid != cred_.uid()) {
        CheckAccess(
            meta_.attr.uid, meta_.attr.gid, meta_.attr.mode,
            cred_, AccessFlags::WRITE);
    }
    meta_.attr.mtime =
        duration_cast<nanoseconds>(mtime.time_since_epoch()).count();
}

void ObjSetattr::setAtime(std::chrono::system_clock::time_point atime)
{
    // The file owner can change the times unconditionally
    if (meta_.attr.uid != cred_.uid()) {
        CheckAccess(
            meta_.attr.uid, meta_.attr.gid, meta_.attr.mode,
            cred_, AccessFlags::WRITE);
    }
    meta_.attr.atime =
        duration_cast<nanoseconds>(atime.time_since_epoch()).count();
}
