/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include <chrono>

#include "filesys/nfs4/nfs4proto.h"

namespace nfsd {
namespace nfs4 {

static inline auto importTime(const filesys::nfs4::nfstime4& t)
{
    using namespace std::chrono;
    auto d = seconds(t.seconds) + nanoseconds(t.nseconds);
    return system_clock::time_point(duration_cast<system_clock::duration>(d));
}

static inline auto exportTime(std::chrono::system_clock::time_point time)
{
    using namespace std::chrono;
    auto d = time.time_since_epoch();
    auto sec = duration_cast<seconds>(d);
    auto nsec = duration_cast<nanoseconds>(d) - sec;
    return filesys::nfs4::nfstime4{
        uint32_t(sec.count()), uint32_t(nsec.count())};
}

static inline auto importDeviceid(const filesys::nfs4::deviceid4& devid)
{
    oncrpc::XdrMemory xm(devid.data(), devid.size());
    uint64_t id;
    xdr(id, static_cast<oncrpc::XdrSource*>(&xm));
    return id;
}

static inline auto exportDeviceid(uint64_t id)
{
    filesys::nfs4::deviceid4 devid;
    oncrpc::XdrMemory xm(devid.data(), devid.size());
    xdr(id, static_cast<oncrpc::XdrSink*>(&xm));
    xdr(uint64_t(0), static_cast<oncrpc::XdrSink*>(&xm));
    return devid;
}

static inline std::shared_ptr<filesys::File> importFileHandle(
    const filesys::nfs4::nfs_fh4& nfh)
{
    filesys::FileHandle fh;
    try {
        oncrpc::XdrMemory xm(const_cast<uint8_t*>(nfh.data()), nfh.size());
        xdr(fh, static_cast<oncrpc::XdrSource*>(&xm));
    }
    catch (oncrpc::XdrError&) {
        throw std::system_error(ESTALE, std::system_category());
    }
    return filesys::FilesystemManager::instance().find(fh);
}

static inline filesys::nfs4::nfs_fh4 exportFileHandle(
    std::shared_ptr<filesys::File> file)
{
    filesys::FileHandle fh = file->handle();
    filesys::nfs4::nfs_fh4 nfh;
    nfh.resize(oncrpc::XdrSizeof(fh));
    oncrpc::XdrMemory xm(nfh.data(), nfh.size());
    xdr(fh, static_cast<oncrpc::XdrSink*>(&xm));
    return nfh;
}

struct Slot;
class NfsSession;

struct CompoundState
{
    Slot* slot = nullptr;
    std::shared_ptr<NfsSession> session;
    int opindex;
    int opcount;
    struct {
        std::shared_ptr<filesys::File> file;
        filesys::nfs4::stateid4 stateid = filesys::nfs4::STATEID_INVALID;
    } curr, save;
};

}
}
