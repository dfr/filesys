/*-
 * Copyright (c) 2016-present Doug Rabson
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

// -*- c++ -*-
#pragma once

#include <iomanip>
#include <iostream>
#include <system_error>
#include <unordered_map>

namespace filesys {
namespace nfs4 {

std::system_error mapStatus(nfsstat4 stat);

template<typename ARRAY>
static int _djb2(const ARRAY& a, int seed = 5381)
{
    size_t hash = seed;
    for (auto c: a)
        hash = (hash << 5) + hash + c; /* hash * 33 + c */
    return hash;
}

struct NfsFhHash
{
    size_t operator()(const filesys::nfs4::nfs_fh4& fh) const
    {
        return _djb2(fh);
    }
};

struct NfsStateidEqualIgnoreSeqid
{
    int operator()(
        const filesys::nfs4::stateid4& x,
        const filesys::nfs4::stateid4& y) const
    {
        return x.other == y.other;
    }
};

struct NfsStateidHashIgnoreSeqid
{
    size_t operator()(const filesys::nfs4::stateid4& stateid) const
    {
        return _djb2(stateid.other);
    }
};

struct NfsClientOwnerHash
{
    size_t operator()(const filesys::nfs4::client_owner4& co) const
    {
        return _djb2(co.co_ownerid, _djb2(co.co_verifier));
    }
};

typedef oncrpc::bounded_vector<std::uint8_t, NFS4_OPAQUE_LIMIT> NfsOwnerId;

struct NfsOwnerIdHash
{
    size_t operator()(const NfsOwnerId& ownerid) const
    {
        return _djb2(ownerid);
    }
};

struct NfsSessionIdHash
{
    size_t operator()(const filesys::nfs4::sessionid4& id) const
    {
        return _djb2(id);
    }
};

// RFC5661 18.16.3: The client can set the clientid field to any value
// and the server MUST ignore it
struct NfsStateOwnerHash
{
    size_t operator()(const filesys::nfs4::state_owner4& owner) const
    {
        return _djb2(owner.owner);
    }
};

static inline int operator==(
    const filesys::nfs4::stateid4& x,
    const filesys::nfs4::stateid4& y)
{
    return x.seqid == y.seqid && x.other == y.other;
}

static inline int operator!=(
    const filesys::nfs4::stateid4& x,
    const filesys::nfs4::stateid4& y)
{
    return x.seqid != y.seqid || x.other != y.other;
}

static inline int operator==(
    const filesys::nfs4::client_owner4& x,
    const filesys::nfs4::client_owner4& y)
{
    return x.co_verifier == y.co_verifier &&
           x.co_ownerid == y.co_ownerid;
}

// RFC5661 18.16.3: The client can set the clientid field to any value
// and the server MUST ignore it
static inline int operator==(
    const filesys::nfs4::state_owner4& x,
    const filesys::nfs4::state_owner4& y)
{
    return x.owner == y.owner;
}

static inline int operator!=(
    const filesys::nfs4::state_owner4& x,
    const filesys::nfs4::state_owner4& y)
{
    return x.owner != y.owner;
}


static inline int operator!=(const nfstime4& x, const nfstime4& y)
{
    return x.seconds != y.seconds || x.nseconds != y.nseconds;
}

static inline std::ostream& operator<<(
    std::ostream& os, const filesys::nfs4::stateid4& stateid)
{
    auto savefill = os.fill();
    auto saveflags = os.flags();
    os << "stateid4{" << stateid.seqid << ",{";
    for (auto c: stateid.other) {
        os << std::hex << std::setw(2) << std::setfill('0') << int(c);
    }
    os << "}}";
    os.fill(savefill);
    os.flags(saveflags);
    return os;
}

static inline std::ostream& operator<<(
    std::ostream& os, const filesys::nfs4::sessionid4& sessionid)
{
    auto savefill = os.fill();
    auto saveflags = os.flags();
    os << "{";
    for (auto b: sessionid) {
        os << std::hex << std::setw(2) << std::setfill('0') << int(b);
    }
    os << "}";
    os.fill(savefill);
    os.flags(saveflags);
    return os;
}

static inline std::ostream& operator<<(
    std::ostream& os, const filesys::nfs4::client_owner4& co)
{
    auto savefill = os.fill();
    auto saveflags = os.flags();
    os << "client_owner4{{";
    for (auto b: co.co_verifier) {
        os << std::hex << std::setw(2) << std::setfill('0') << int(b);
    }
    os << "},{";
    for (auto b: co.co_ownerid) {
        os << char(b);
    }
    os << "}}";
    os.fill(savefill);
    os.flags(saveflags);
    return os;
}

static inline std::ostream& operator<<(
    std::ostream& os, const filesys::nfs4::state_owner4& so)
{
    auto savefill = os.fill();
    auto saveflags = os.flags();
    os << std::hex;
    os << "state_owner4{";
    os << so.clientid;
    os << ",{";
    for (auto b: so.owner) {
        os << std::setw(2) << std::setfill('0') << int(b);
    }
    os << "}}";
    os.fill(savefill);
    os.flags(saveflags);
    return os;
}

static inline std::ostream& operator<<(
    std::ostream& os, const filesys::nfs4::nfs_fh4& fh)
{
    auto savefill = os.fill();
    auto saveflags = os.flags();
    os << "nfs_fh4{";
    for (auto b: fh) {
        os << std::hex << std::setw(2) << std::setfill('0') << int(b);
    }
    os << "}";
    os.fill(savefill);
    os.flags(saveflags);
    return os;
}

static inline std::string toString(const utf8string& us)
{
    return std::string(reinterpret_cast<const char*>(us.data()), us.size());
}

static inline utf8string toUtf8string(const std::string& s)
{
    utf8string us(s.size());
    std::copy_n(s.data(), s.size(), us.data());
    return us;
}

static inline void set(bitmap4& bm, int bit)
{
    int word = bit / 32;
    bit = bit & 31;
    while (word >= int(bm.size()))
        bm.push_back(0);
    bm[word] |= (1 << bit);
}

static inline bitmap4& operator+=(bitmap4& bm, int bit)
{
    set(bm, bit);
    return bm;
}

static inline void clear(bitmap4& bm, int bit)
{
    int word = bit / 32;
    bit = bit & 31;
    while (word >= int(bm.size()))
        bm.push_back(0);
    bm[word] &= ~(1 << bit);
}

static inline bitmap4& operator-=(bitmap4& bm, int bit)
{
    clear(bm, bit);
    return bm;
}

static inline bool isset(const bitmap4& bm, int bit)
{
    int word = bit / 32;
    bit = bit & 31;
    if (word >= int(bm.size()))
        return false;
    return (bm[word] & (1 << bit)) != 0;
}

static inline bitmap4& operator&=(bitmap4& bm, const bitmap4& mask)
{
    for (int i = 0; i < int(bm.size()); i++) {
        if (i >= int(mask.size()))
            bm[i] = 0;
        else
            bm[i] &= mask[i];
    }
    return bm;
}

static inline int firstSetBit(uint32_t set)
{
    // for i in xrange(256):
    //     if (i & 15) == 0:
    //         print '       ',
    //     if i == 0:
    //         print '0,',
    //     else:
    //         for j in xrange(8):
    //             if i & (1 << j):
    //                 print '%d,' % j,
    //                 break
    //     if (i & 15) == 15:
    //         print
    static uint8_t table[256] = {
        0, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    };
    assert(set != 0);
    if (set & 0x000000ff)
        return table[set & 0xff];
    if (set & 0x0000ff00)
        return table[(set >> 8) & 0xff] + 8;
    if (set & 0x00ff0000)
        return table[(set >> 16) & 0xff] + 16;
    return table[(set >> 24) & 0xff] + 24;
}

}
}
