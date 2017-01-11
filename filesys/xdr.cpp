/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#include <system_error>

#include <filesys/filesys.h>
#include <glog/logging.h>

using namespace filesys;

void filesys::xdr(const FileHandle& v, oncrpc::XdrSink* xdrs)
{
    xdr(v.version, xdrs);
    xdr(v.handle, xdrs);
}

void filesys::xdr(FileHandle& v, oncrpc::XdrSource* xdrs)
{
    xdr(v.version, xdrs);
    if (v.version != 1) {
        if (v.version != 1) {
            LOG(ERROR) << "unexpected file handle version: "
                       << v.version << ", expected 1";
            throw std::system_error(ESTALE, std::system_category());
        }
    }
    uint32_t len;
    xdr(len, xdrs);
    if (len > 256) {
        LOG(ERROR) << "Corrupt wire format filehandle";
        throw std::system_error(ESTALE, std::system_category());
    }
    v.handle.resize(len);
    xdrs->getBytes(v.handle.data(), len);
}
