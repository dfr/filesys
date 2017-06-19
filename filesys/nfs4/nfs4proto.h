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

#include <rpc++/rpcproto.h>

namespace filesys {
namespace nfs4 {

using oncrpc::AUTH_NONE;
using oncrpc::AUTH_SYS;
using oncrpc::RPCSEC_GSS;

struct authsys_parms
{
    uint32_t stamp;
    std::string machinename;
    int32_t uid;
    int32_t gid;
    std::vector<int32_t> gids;
};

template <typename XDR>
static void xdr(oncrpc::RefType<authsys_parms, XDR> v, XDR* xdrs)
{
    xdr(v.stamp, xdrs);
    xdr(v.machinename, xdrs);
    xdr(v.uid, xdrs);
    xdr(v.gid, xdrs);
    xdr(v.gids, xdrs);
}

}
}

#define _AUTH_SYS_DEFINE_FOR_NFSv41

#include "filesys/proto/nfs4_prot.h"
#include "filesys/proto/flex_files_layout.h"

namespace filesys {
namespace nfs4 {

constexpr stateid4 STATEID_ANON = { 0u, {{0,0,0,0, 0,0,0,0, 0,0,0,0}} };
constexpr stateid4 STATEID_LAST = { 1u, {{0,0,0,0, 0,0,0,0, 0,0,0,0}} };
constexpr stateid4 STATEID_BYPASS =
    { ~0u, {{0xff,0xff,0xff,0xff, 0xff,0xff,0xff,0xff, 0xff,0xff,0xff,0xff}} };
constexpr stateid4 STATEID_INVALID = { ~0u, {{0,0,0,0, 0,0,0,0, 0,0,0,0}} };

}
}
