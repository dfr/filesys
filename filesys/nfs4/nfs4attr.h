// -*- c++ -*-
#pragma once

#include <rpc++/xdr.h>

#include "nfs4proto.h"

namespace filesys {
namespace nfs4 {

void setSupportedAttrs(bitmap4& wanted);

static inline auto fromNfsTime(const nfstime4& t)
{
    using namespace std::chrono;
    auto d = seconds(t.seconds) + nanoseconds(t.nseconds);
    return system_clock::time_point(duration_cast<system_clock::duration>(d));
}

static inline auto toNfsTime(std::chrono::system_clock::time_point time)
{
    using namespace std::chrono;
    auto d = time.time_since_epoch();
    auto sec = duration_cast<seconds>(d);
    auto nsec = duration_cast<nanoseconds>(d) - sec;
    return nfstime4{uint32_t(sec.count()), uint32_t(nsec.count())};
}

struct NfsAttr
{
    NfsAttr() {}
    NfsAttr(const fattr4& attr)
    {
        decode(attr);
    }

    void decode(const fattr4& attr);
    void encode(fattr4& attr);

    bitmap4 attrmask_;

    fattr4_supported_attrs supported_attrs_;
    fattr4_change change_ = 0;
    fattr4_filehandle filehandle_;
    fattr4_type type_ = NF4REG;
    fattr4_mode mode_ = 0;
    fattr4_numlinks numlinks_ = 0;
    fattr4_owner owner_;
    fattr4_owner_group owner_group_;
    fattr4_size size_ = 0;
    fattr4_space_used space_used_ = 0;
    fattr4_fsid fsid_ = {0, 0};
    fattr4_fileid fileid_ = 0;
    time_how4 time_access_set_ = SET_TO_CLIENT_TIME4;
    fattr4_time_access time_access_ = {0, 0};
    fattr4_time_create time_create_ = {0, 0};
    time_how4 time_modify_set_ = SET_TO_CLIENT_TIME4;
    fattr4_time_modify time_modify_ = {0, 0};
    fattr4_time_metadata time_metadata_ = {0, 0};
    fattr4_files_avail files_avail_ = 0;
    fattr4_files_free files_free_ = 0;
    fattr4_files_total files_total_ = 0;
    fattr4_space_avail space_avail_ = 0;
    fattr4_space_free space_free_ = 0;
    fattr4_space_total space_total_ = 0;
    fattr4_maxread maxread_ = 0;
    fattr4_maxread maxwrite_ = 0;

    // Used for reporting constant-valued attributes
    bool true_ = true;
    bool false_ = false;
};

template <typename XDR>
static void xdr(oncrpc::RefType<NfsAttr, XDR> v, XDR* xdrs)
{
    int i = 0;
    for (auto word: v.attrmask_) {
        while (word) {
            int j = firstSetBit(word);
            word ^= (1 << j);
            switch (i + j) {
            case FATTR4_SUPPORTED_ATTRS:
                xdr(v.supported_attrs_, xdrs);
                break;
            case FATTR4_CHANGE:
                xdr(v.change_, xdrs);
                break;
            case FATTR4_FILEHANDLE:
                xdr(v.filehandle_, xdrs);
                break;
            case FATTR4_TYPE:
                xdr(v.type_, xdrs);
                break;
            case FATTR4_MODE:
                xdr(v.mode_, xdrs);
                break;
            case FATTR4_NUMLINKS:
                xdr(v.numlinks_, xdrs);
                break;
            case FATTR4_OWNER:
                xdr(v.owner_, xdrs);
                break;
            case FATTR4_OWNER_GROUP:
                xdr(v.owner_group_, xdrs);
                break;
            case FATTR4_SIZE:
                xdr(v.size_, xdrs);
                break;
            case FATTR4_SPACE_USED:
                xdr(v.space_used_, xdrs);
                break;
            case FATTR4_FSID:
                xdr(v.fsid_, xdrs);
                break;
            case FATTR4_FILEID:
                xdr(v.fileid_, xdrs);
                break;
            case FATTR4_TIME_ACCESS:
                xdr(v.time_access_, xdrs);
                break;
            case FATTR4_TIME_ACCESS_SET: {
                xdr(v.time_access_set_, xdrs);
                if (v.time_access_set_ == SET_TO_CLIENT_TIME4)
                    xdr(v.time_access_, xdrs);
                break;
            }
            case FATTR4_TIME_CREATE:
                xdr(v.time_create_, xdrs);
                break;
            case FATTR4_TIME_MODIFY:
                xdr(v.time_modify_, xdrs);
                break;
            case FATTR4_TIME_MODIFY_SET: {
                xdr(v.time_modify_set_, xdrs);
                if (v.time_modify_set_ == SET_TO_CLIENT_TIME4)
                    xdr(v.time_modify_, xdrs);
                break;
            }
            case FATTR4_TIME_METADATA:
                xdr(v.time_metadata_, xdrs);
                break;
            case FATTR4_FILES_AVAIL:
                xdr(v.files_avail_, xdrs);
                break;
            case FATTR4_FILES_FREE:
                xdr(v.files_free_, xdrs);
                break;
            case FATTR4_FILES_TOTAL:
                xdr(v.files_total_, xdrs);
                break;
            case FATTR4_SPACE_AVAIL:
                xdr(v.space_avail_, xdrs);
                break;
            case FATTR4_SPACE_FREE:
                xdr(v.space_free_, xdrs);
                break;
            case FATTR4_SPACE_TOTAL:
                xdr(v.space_total_, xdrs);
                break;
            case FATTR4_MAXREAD:
                xdr(v.maxread_, xdrs);
                break;
            case FATTR4_MAXWRITE:
                xdr(v.maxwrite_, xdrs);
                break;
            case FATTR4_LINK_SUPPORT:
            case FATTR4_SYMLINK_SUPPORT:
            case FATTR4_UNIQUE_HANDLES:
                xdr(v.true_, xdrs);
                break;
            case FATTR4_NAMED_ATTR:
                xdr(v.false_, xdrs);
                break;
            default:
                abort();
            }
        }
        i += 32;
    }
}

}
}
