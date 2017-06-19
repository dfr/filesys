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

#include "nfs4ds.h"

using namespace filesys;
using namespace filesys::distfs;
using namespace filesys::nfs4;
using namespace std;

NfsDataStore::NfsDataStore(
    std::shared_ptr<oncrpc::Channel> chan,
    std::shared_ptr<oncrpc::Client> client,
    std::shared_ptr<util::Clock> clock,
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
