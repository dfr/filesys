/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include "nfs4ds.h"

using namespace filesys;
using namespace filesys::distfs;
using namespace filesys::nfs4;
using namespace std;

NfsDataStore::NfsDataStore(
    std::shared_ptr<oncrpc::Channel> chan,
    std::shared_ptr<oncrpc::Client> client,
    std::shared_ptr<detail::Clock> clock,
    const std::string& clientowner)
    : fs_(make_shared<NfsFilesystem>(chan, client, clock, clientowner)),
      ds_(make_shared<dsclientT>(chan))
{
    fs_->root();
}

shared_ptr<File> NfsDataStore::findPiece(
    const Credential& cred, const PieceId& id)
{
    auto res = ds_->findPiece(
        FINDPIECEargs{id.fileid, id.offset, id.size});
    if (res.status == DISTFS_OK)
        return fs_->find(res.resok().object);
    throw system_error(res.status, system_category());
}

shared_ptr<File> NfsDataStore::createPiece(
    const Credential& cred, const PieceId& id)
{
    auto res = ds_->createPiece(
        CREATEPIECEargs{id.fileid, id.offset, id.size});
    if (res.status == DISTFS_OK)
        return fs_->find(res.resok().object);
    throw system_error(res.status, system_category());
}

void NfsDataStore::removePiece(
    const Credential& cred, const PieceId& id)
{
    auto res = ds_->removePiece(
        REMOVEPIECEargs{id.fileid, id.offset, id.size});
    if (res.status == DISTFS_OK)
        return;
    throw system_error(res.status, system_category());
}
