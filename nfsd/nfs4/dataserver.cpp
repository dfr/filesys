/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <iomanip>

#include <fs++/filesys.h>
#include <rpc++/cred.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "dataserver.h"

using namespace filesys;
using namespace filesys::distfs;
using namespace nfsd;
using namespace nfsd::nfs4;
using namespace oncrpc;
using namespace std;

#if 0
static inline std::ostream& operator<<(std::ostream& os, const FileHandle& fh)
{
    auto savefill = os.fill();
    auto saveflags = os.flags();
    os << "FileHandle{" << fh.version;
    os << ",{";
    for (auto c: fh.handle) {
        os << std::hex << std::setw(2) << std::setfill('0') << int(c);
    }
    os << "}}";
    os.fill(savefill);
    os.flags(saveflags);
    return os;
}
#endif

static inline std::ostream& operator<<(
    std::ostream& os, const distfs_fh& fh)
{
    auto savefill = os.fill();
    auto saveflags = os.flags();
    os << "distfs_fh{";
    for (auto b: fh) {
        os << std::hex << std::setw(2) << std::setfill('0') << int(b);
    }
    os << "}";
    os.fill(savefill);
    os.flags(saveflags);
    return os;
}

static distfs_fh exportFileHandle(const FileHandle& fh)
{
    distfs_fh dfh;
    dfh.resize(XdrSizeof(fh));
    XdrMemory xm(dfh.data(), dfh.size());
    xdr(fh, static_cast<XdrSink*>(&xm));
    return dfh;
}

DataServer::DataServer(const vector<int>& sec)
    : sec_(sec)
{
}

void DataServer::null()
{
}

FINDPIECEres DataServer::findPiece(const FINDPIECEargs& args)
{
    auto& cred = CallContext::current().cred();
    VLOG(1) << "DataServer::findPiece("
            << args.fileid
            << ", " << args.offset << ")";
    try {
        auto ds = dynamic_pointer_cast<DataStore>(
            FilesystemManager::instance().begin()->second);
        if (ds) {
            PieceId id{FileId(args.fileid), args.offset, args.size};
            auto file = ds->findPiece(cred, id);
            FileHandle fh;
            file->handle(fh);
            auto nfh = exportFileHandle(fh);
            VLOG(1) << "Returning handle: " << nfh;
            return FINDPIECEres(DISTFS_OK, FINDPIECEresok{nfh});
        }
        return FINDPIECEres(DISTFSERR_NOENT);
    }
    catch (system_error& e) {
        return FINDPIECEres(distfsstat(e.code().value()));
    }
}

CREATEPIECEres DataServer::createPiece(const CREATEPIECEargs& args)
{
    auto& cred = CallContext::current().cred();
    VLOG(1) << "DataServer::createPiece("
            << args.fileid
            << ", " << args.offset << ")";
    try {
        auto ds = dynamic_pointer_cast<DataStore>(
            FilesystemManager::instance().begin()->second);
        if (ds) {
            PieceId id{FileId(args.fileid), args.offset, args.size};
            auto file = ds->createPiece(cred, id);
            FileHandle fh;
            file->handle(fh);
            auto nfh = exportFileHandle(fh);
            VLOG(1) << "Returning handle: " << nfh;

            Credential cred{0, 0, {}, true};
            auto fsattr = ds->root()->fsstat(cred);
            StorageStatus storage;
            storage.totalSpace = fsattr->tbytes();
            storage.freeSpace = fsattr->fbytes();
            storage.availSpace = fsattr->abytes();

            return CREATEPIECEres(DISTFS_OK, CREATEPIECEresok{nfh, storage});
        }
        return CREATEPIECEres(DISTFSERR_NOENT);
    }
    catch (system_error& e) {
        return CREATEPIECEres(distfsstat(e.code().value()));
    }
}

REMOVEPIECEres DataServer::removePiece(const REMOVEPIECEargs& args)
{
    auto& cred = CallContext::current().cred();
    VLOG(1) << "DataServer::removePiece("
            << args.fileid
            << ", " << args.offset << ")";
    try {
        auto ds = dynamic_pointer_cast<DataStore>(
            FilesystemManager::instance().begin()->second);
        if (ds) {
            PieceId id{FileId(args.fileid), args.offset, args.size};
            ds->removePiece(cred, id);
            return REMOVEPIECEres{DISTFS_OK};
        }
        return REMOVEPIECEres{DISTFSERR_NOENT};
    }
    catch (system_error& e) {
        LOG(ERROR) << "removePiece(" << args.fileid
                   << ", " << args.offset
                   << ") failed: " << e.what();
        return REMOVEPIECEres{distfsstat(e.code().value())};
    }
}
