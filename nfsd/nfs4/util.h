/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include <chrono>

#include "filesys/nfs4/nfs4proto.h"

namespace nfsd {
namespace nfs4 {

static inline char toHexChar(int digit)
{
    static char hex[] = {
        '0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'
    };
    return hex[digit];
}

static inline int fromHexChar(char ch)
{
    if (ch >= '0' && ch <= '9')
        return ch - '0';
    if (ch >= 'a' && ch <= 'f')
        return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F')
        return ch - 'A' + 10;
    throw std::system_error(ENOENT, std::system_category());
}

static inline std::string toHexClientid(uint64_t id)
{
    std::string res;
    res.resize(16);
    for (int i = 0; i < 16; i++) {
        res[i] = toHexChar(id >> 60);
        id <<= 4;
    }
    return res;
}

static inline std::string toHexSessionid(const filesys::nfs4::sessionid4& id)
{
    std::string res;
    res.resize(2*filesys::nfs4::NFS4_SESSIONID_SIZE);
    int i = 0;
    for (auto c: id) {
        res[i] = toHexChar((c >> 4) & 15);
        res[i+1] = toHexChar(c & 15);
        i += 2;
    }
    return res;
}

static inline std::string toHexStateid(const filesys::nfs4::stateid4& id)
{
    std::string res;
    res.resize(24);
    int i = 0;
    for (auto c: id.other) {
        res[i] = toHexChar((c >> 4) & 15);
        res[i+1] = toHexChar(c & 15);
        i += 2;
    }
    return res;
}

static inline std::string toHexFileHandle(const filesys::nfs4::nfs_fh4& fh)
{
    std::string res;
    res.resize(2*fh.size());
    int i = 0;
    for (auto c: fh) {
        res[i] = toHexChar((c >> 4) & 15);
        res[i+1] = toHexChar(c & 15);
        i += 2;
    }
    return res;
}

static inline filesys::nfs4::stateid4 fromHexStateid(const std::string& s)
{
    if (s.size() != 24)
        throw std::system_error(ENOENT, std::system_category());

    filesys::nfs4::stateid4 id;
    id.seqid = 0;
    for (int i = 0; i < 12; i++) {
        int hi = fromHexChar(s[2*i]);
        int lo = fromHexChar(s[2*i + 1]);
        id.other[i] = hi * 16 + lo;
    }
    return id;
}

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
