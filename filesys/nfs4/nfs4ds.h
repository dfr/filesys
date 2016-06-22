/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include "nfs4fs.h"
#include "filesys/distfs/distfsproto.h"

namespace filesys {
namespace nfs4 {

class NfsDataStore: public DataStore,
                    public std::enable_shared_from_this<NfsDataStore>
{
public:
    NfsDataStore(
        std::shared_ptr<oncrpc::Channel> chan,
        std::shared_ptr<oncrpc::Client> client,
        std::shared_ptr<detail::Clock> clock,
        const std::string& clientowner);

    // Filesystem overrides
    std::shared_ptr<File> root() override
    {
        return fs_->root();
    }
    const FilesystemId& fsid() const override
    {
        return fs_->fsid();
    }
    std::shared_ptr<File> find(const FileHandle& fh) override
    {
        return fs_->find(exportFileHandle(fh));
    }
    bool isData() const override {
        return true;
    }

    // DataStore overrides
    std::shared_ptr<File> findPiece(
        const Credential& cred, const PieceId& id) override;
    std::shared_ptr<File> createPiece(
        const Credential& cred, const PieceId& id) override;
    void removePiece(
        const Credential& cred, const PieceId& id) override;

private:
    typedef filesys::distfs::DistfsDs1<oncrpc::SysClient> dsclientT;

    static nfs_fh4 exportFileHandle(const FileHandle& fh)
    {
        using namespace oncrpc;
        nfs_fh4 nfh;
        nfh.resize(XdrSizeof(fh));
        XdrMemory xm(nfh.data(), nfh.size());
        xdr(fh, static_cast<XdrSink*>(&xm));
        return nfh;
    }

    static FileHandle importFileHandle(const nfs_fh4& nfh)
    {
        using namespace oncrpc;
        FileHandle fh;
        try {
            XdrMemory xm(const_cast<uint8_t*>(nfh.data()), nfh.size());
            xdr(fh, static_cast<XdrSource*>(&xm));
        }
        catch (XdrError&) {
            throw std::system_error(ESTALE, std::system_category());
        }
        return fh;
    }

    std::shared_ptr<NfsFilesystem> fs_;
    std::shared_ptr<dsclientT> ds_;
};

}
}
