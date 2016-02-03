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
    uint32_t uid;
    uint32_t gid;
    std::vector<uint32_t> gids;
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

#include <fs++/proto/nfs4_prot.h>

namespace filesys {
namespace nfs4 {

constexpr stateid4 STATEID_ANON = { 0u, {{0,0,0,0, 0,0,0,0, 0,0,0,0}} };
constexpr stateid4 STATEID_LAST = { 1u, {{0,0,0,0, 0,0,0,0, 0,0,0,0}} };
constexpr stateid4 STATEID_BYPASS =
    { ~0u, {{0xff,0xff,0xff,0xff, 0xff,0xff,0xff,0xff, 0xff,0xff,0xff,0xff}} };
constexpr stateid4 STATEID_INVALID = { ~0u, {{0,0,0,0, 0,0,0,0, 0,0,0,0}} };

}
}
