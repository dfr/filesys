/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#pragma once

#include "filesys/distfs/distfsproto.h"

namespace nfsd {
namespace nfs4 {

class DataServer: public filesys::distfs::DistfsDs1Service
{
public:
    DataServer(const std::vector<int>& sec);

    // IDistfsDs1 overrides
    void null() override;
    filesys::distfs::FINDPIECEres findPiece(
        const filesys::distfs::FINDPIECEargs& args) override;
    filesys::distfs::CREATEPIECEres createPiece(
        const filesys::distfs::CREATEPIECEargs& args) override;
    filesys::distfs::REMOVEPIECEres removePiece(
        const filesys::distfs::REMOVEPIECEargs& args) override;

private:
    std::vector<int> sec_;
};

}
}
