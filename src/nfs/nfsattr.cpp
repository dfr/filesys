#include <fs++/filesys.h>
#include <fs++/nfsfs.h>
#include <fs++/urlparser.h>
#include <glog/logging.h>

#include "src/nfs/nfsfs.h"

using namespace filesys;
using namespace filesys::nfs;
using namespace std::chrono;

static auto fromNfsTime(const nfstime3& t)
{
    auto d = seconds(t.seconds) + nanoseconds(t.nseconds);
    return system_clock::time_point(duration_cast<system_clock::duration>(d));
}

static auto toNfsTime(system_clock::time_point time)
{
    auto d = time.time_since_epoch();
    auto sec = duration_cast<seconds>(d);
    auto nsec = duration_cast<nanoseconds>(d) - sec;
    return nfstime3{uint32(sec.count()), uint32(nsec.count())};
}

NfsGetattr::NfsGetattr(const fattr3& attr)
    : attr_(attr)
{
}

FileType NfsGetattr::type() const
{
    switch (attr_.type) {
    case NF3REG:
        return FileType::FILE;
    case NF3DIR:
        return FileType::DIRECTORY;
    case NF3BLK:
        return FileType::BLOCKDEV;
    case NF3CHR:
        return FileType::CHARDEV;
    case NF3LNK:
        return FileType::SYMLINK;
    case NF3SOCK:
        return FileType::SOCKET;
    case NF3FIFO:
        return FileType::FIFO;
    }
}

int NfsGetattr::mode() const
{
    return attr_.mode;
}

int NfsGetattr::nlink() const
{
    return attr_.nlink;
}

int NfsGetattr::uid() const
{
    return attr_.uid;
}

int NfsGetattr::gid() const
{
    return attr_.gid;
}

std::uint64_t NfsGetattr::size() const
{
    return attr_.size;
}

std::uint64_t NfsGetattr::used() const
{
    return attr_.used;
}

std::uint64_t NfsGetattr::fileid() const
{
    return attr_.fileid;
}

std::chrono::system_clock::time_point NfsGetattr::mtime() const
{
    return  fromNfsTime(attr_.mtime);
}

std::chrono::system_clock::time_point NfsGetattr::atime() const
{
    return  fromNfsTime(attr_.atime);
}

std::chrono::system_clock::time_point NfsGetattr::ctime() const
{
    return  fromNfsTime(attr_.ctime);
}

void NfsSettattr::setMode(int mode)
{
    attr_.mode.set_set_it(true);
    attr_.mode.mode() = mode;
}

void NfsSettattr::setUid(int uid)
{
    attr_.uid.set_set_it(true);
    attr_.uid.uid() = uid;
}

void NfsSettattr::setGid(int gid)
{
    attr_.gid.set_set_it(true);
    attr_.gid.gid() = gid;
}

void NfsSettattr::setSize(std::uint64_t size)
{
    attr_.size.set_set_it(true);
    attr_.size.size() = size;
}

void NfsSettattr::setMtime(std::chrono::system_clock::time_point mtime)
{
    attr_.mtime.set_set_it(SET_TO_CLIENT_TIME);
    attr_.mtime.mtime() = toNfsTime(mtime);
}

void NfsSettattr::setAtime(std::chrono::system_clock::time_point atime)
{
    attr_.atime.set_set_it(SET_TO_CLIENT_TIME);
    attr_.atime.atime() = toNfsTime(atime);
}
