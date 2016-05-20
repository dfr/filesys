#include <pwd.h>
#include <grp.h>

#include <fs++/filesys.h>
#include <fs++/urlparser.h>
#include <glog/logging.h>

#include "nfs4fs.h"

using namespace filesys;
using namespace filesys::nfs4;
using namespace std;
using namespace std::chrono;
using namespace std::literals;

void filesys::nfs4::setSupportedAttrs(bitmap4& wanted)
{
    set(wanted, FATTR4_CHANGE);
    set(wanted, FATTR4_TYPE);
    set(wanted, FATTR4_MODE);
    set(wanted, FATTR4_NUMLINKS);
    set(wanted, FATTR4_OWNER);
    set(wanted, FATTR4_OWNER_GROUP);
    set(wanted, FATTR4_SIZE);
    set(wanted, FATTR4_SPACE_USED);
    set(wanted, FATTR4_FSID);
    set(wanted, FATTR4_FILEID);
    set(wanted, FATTR4_TIME_ACCESS);
    set(wanted, FATTR4_TIME_CREATE);
    set(wanted, FATTR4_TIME_MODIFY);
    set(wanted, FATTR4_TIME_METADATA);
}

void NfsAttr::decode(const fattr4& attr)
{
    oncrpc::XdrMemory xm(attr.attr_vals.data(), attr.attr_vals.size());
    auto xdrs = static_cast<oncrpc::XdrSource*>(&xm);
    attrmask_ = attr.attrmask;
    xdr(*this, xdrs);
}

void NfsAttr::encode(fattr4& attr)
{
    attr.attrmask = attrmask_;
    attr.attr_vals.resize(oncrpc::XdrSizeof(*this));
    oncrpc::XdrMemory xm(attr.attr_vals.data(), attr.attr_vals.size());
    auto xdrs = static_cast<oncrpc::XdrSink*>(&xm);
    xdr(*this, xdrs);
}

FileType NfsGetattr::type() const
{
    switch (attr_.type_) {
    case NF4REG:
        return FileType::FILE;
    case NF4DIR:
        return FileType::DIRECTORY;
    case NF4BLK:
        return FileType::BLOCKDEV;
    case NF4CHR:
        return FileType::CHARDEV;
    case NF4LNK:
        return FileType::SYMLINK;
    case NF4SOCK:
        return FileType::SOCKET;
    case NF4FIFO:
        return FileType::FIFO;
    case NF4ATTRDIR:
        return FileType::DIRECTORY;
    case NF4NAMEDATTR:
        return FileType::FILE;
    }
}

int NfsGetattr::mode() const
{
    return attr_.mode_;
}

int NfsGetattr::nlink() const
{
    return attr_.numlinks_;
}

int NfsGetattr::uid() const
{
    return idmapper_->toUid(toString(attr_.owner_));
}

int NfsGetattr::gid() const
{
    return idmapper_->toGid(toString(attr_.owner_group_));
}

std::uint64_t NfsGetattr::size() const
{
    return attr_.size_;
}

std::uint64_t NfsGetattr::used() const
{
    return attr_.space_used_;
}

std::uint32_t NfsGetattr::blockSize() const
{
    return 4096;
}

FileId NfsGetattr::fileid() const
{
    return FileId(attr_.fileid_);
}

std::chrono::system_clock::time_point NfsGetattr::mtime() const
{
    return  fromNfsTime(attr_.time_modify_);
}

std::chrono::system_clock::time_point NfsGetattr::atime() const
{
    return  fromNfsTime(attr_.time_access_);
}

std::chrono::system_clock::time_point NfsGetattr::ctime() const
{
    return  fromNfsTime(attr_.time_metadata_);
}

std::chrono::system_clock::time_point NfsGetattr::birthtime() const
{
    return  fromNfsTime(attr_.time_create_);
}

std::uint64_t NfsGetattr::change() const
{
    auto& t = attr_.time_metadata_;
    return (std::uint64_t(t.seconds) << 32) | t.nseconds;
}

std::uint64_t NfsGetattr::createverf() const
{
    return 0;
}

void NfsSetattr::setMode(int mode)
{
    set(attrmask_, FATTR4_MODE);
    mode_ = mode;
}

void NfsSetattr::setUid(int uid)
{
    //attr_.uid.set_set_it(true);
    //attr_.uid.uid() = uid;
}

void NfsSetattr::setGid(int gid)
{
    //attr_.gid.set_set_it(true);
    //attr_.gid.gid() = gid;
}

void NfsSetattr::setSize(std::uint64_t size)
{
    set(attrmask_, FATTR4_SIZE);
    size_ = size;
}

void NfsSetattr::setMtime(std::chrono::system_clock::time_point mtime)
{
    set(attrmask_, FATTR4_TIME_MODIFY_SET);
    time_modify_ = toNfsTime(mtime);
}

void NfsSetattr::setAtime(std::chrono::system_clock::time_point atime)
{
    set(attrmask_, FATTR4_TIME_ACCESS_SET);
    time_access_ = toNfsTime(atime);
}

void NfsSetattr::setChange(std::uint64_t change)
{
}

void NfsSetattr::setCreateverf(std::uint64_t verf)
{
}

#if 0
NfsFsattr::NfsFsattr(const FSSTAT3resok& stat, const PATHCONF3resok& pc)
    : tbytes_(stat.tbytes),
      fbytes_(stat.fbytes),
      abytes_(stat.abytes),
      tfiles_(stat.tfiles),
      ffiles_(stat.ffiles),
      afiles_(stat.afiles),
      linkMax_(pc.linkmax),
      nameMax_(pc.name_max)
{
}

#endif
