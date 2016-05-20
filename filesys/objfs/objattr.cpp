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
    switch (attr_.type) {
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
    return attr_.mode;
}

int ObjGetattr::nlink() const
{
    return attr_.nlink;
}

int ObjGetattr::uid() const
{
    return attr_.uid;
}

int ObjGetattr::gid() const
{
    return attr_.gid;
}

std::uint64_t ObjGetattr::size() const
{
    return attr_.size;
}

std::uint64_t ObjGetattr::used() const
{
    return used_();
}

std::uint32_t ObjGetattr::blockSize() const
{
    return blockSize_;
}

FileId ObjGetattr::fileid() const
{
    return fileid_;
}

std::chrono::system_clock::time_point ObjGetattr::mtime() const
{
    return system_clock::time_point(
        duration_cast<system_clock::duration>(nanoseconds(attr_.mtime)));
}

std::chrono::system_clock::time_point ObjGetattr::atime() const
{
    return system_clock::time_point(
        duration_cast<system_clock::duration>(nanoseconds(attr_.atime)));
}

std::chrono::system_clock::time_point ObjGetattr::ctime() const
{
    return system_clock::time_point(
        duration_cast<system_clock::duration>(nanoseconds(attr_.ctime)));
}

std::chrono::system_clock::time_point ObjGetattr::birthtime() const
{
    return system_clock::time_point(
        duration_cast<system_clock::duration>(
            nanoseconds(attr_.birthtime)));
}

std::uint64_t ObjGetattr::change() const
{
    return attr_.ctime;
}

std::uint64_t ObjGetattr::createverf() const
{
    return attr_.atime;
}

void ObjSetattr::setMode(int mode)
{
    if (cred_.uid() != attr_.uid && !cred_.privileged())
        throw system_error(EPERM, system_category());
    attr_.mode = mode;
}

void ObjSetattr::setUid(int uid)
{
    if (uid != attr_.uid && !cred_.privileged())
        throw system_error(EPERM, system_category());
    attr_.uid = uid;
}

void ObjSetattr::setGid(int gid)
{
    // If the cred matches the file owner and contains the requested group,
    // or the cred is privileged, we can change the file group
    if ((cred_.uid() == attr_.uid && cred_.hasgroup(gid))
        || cred_.privileged())
        attr_.gid = gid;
    else
        throw system_error(EPERM, system_category());
}

void ObjSetattr::setSize(std::uint64_t size)
{
    // FreeBSD allows set size if cred uid is the same as file uid and
    // iozone seems to expect this
    if (cred_.uid() != attr_.uid) {
        CheckAccess(
            attr_.uid, attr_.gid, attr_.mode,
            cred_, AccessFlags::WRITE);
    }
    attr_.size = size;
}

void ObjSetattr::setMtime(std::chrono::system_clock::time_point mtime)
{
    // The file owner can change the times unconditionally
    if (attr_.uid != cred_.uid()) {
        CheckAccess(
            attr_.uid, attr_.gid, attr_.mode,
            cred_, AccessFlags::WRITE);
    }
    attr_.mtime =
        duration_cast<nanoseconds>(mtime.time_since_epoch()).count();
}

void ObjSetattr::setAtime(std::chrono::system_clock::time_point atime)
{
    // The file owner can change the times unconditionally
    if (attr_.uid != cred_.uid()) {
        CheckAccess(
            attr_.uid, attr_.gid, attr_.mode,
            cred_, AccessFlags::WRITE);
    }
    attr_.atime =
        duration_cast<nanoseconds>(atime.time_since_epoch()).count();
}

void ObjSetattr::setChange(std::uint64_t change)
{
    // The file owner can change the times unconditionally
    if (attr_.uid != cred_.uid()) {
        CheckAccess(
            attr_.uid, attr_.gid, attr_.mode,
            cred_, AccessFlags::WRITE);
    }
    // Enforce monotonicity
    if (change > attr_.ctime)
        attr_.ctime = change;
}

void ObjSetattr::setCreateverf(std::uint64_t verf)
{
    // The file owner can change the times unconditionally
    if (attr_.uid != cred_.uid()) {
        CheckAccess(
            attr_.uid, attr_.gid, attr_.mode,
            cred_, AccessFlags::WRITE);
    }
    attr_.atime = verf;
}
