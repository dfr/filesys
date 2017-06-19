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

#include <cassert>
#include <iostream>
#include <system_error>

#include <rpc++/xdr.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "distfsck.h"
#include "filesys/nfs4/nfs4ds.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace filesys::distfs;
using namespace keyval;
using namespace std;

DEFINE_string(realm, "", "Local krb5 realm name");

void DistfsCheck::check()
{
    ObjfsCheck::check(false);

    auto devicesNS = db_->getNamespace("devices");
    auto dataNS = db_->getNamespace("data");
    auto piecesNS = db_->getNamespace("pieces");

    auto clock = make_shared<util::SystemClock>();

    // Read the devices table so that we can verify pieces
    auto iterator = devicesNS->iterator();
    map<int, shared_ptr<nfs4::NfsDataStore>> servers;
    while (iterator->valid()) {
        auto k = iterator->key();
        auto v = iterator->value();

        auto dsid = KeyType(k).id();
        DeviceStatus ds;
        oncrpc::XdrMemory xm(v->data(), v->size());
        xdr(ds, static_cast<oncrpc::XdrSource*>(&xm));

        for (auto& uaddr: ds.uaddrs) {
            try {
                auto ai = oncrpc::AddressInfo::fromUaddr(uaddr, "tcp");
                LOG(INFO) << "Server " << dsid << ": connecting: "
                          << ai.host() << ":" << ai.port();
                auto chan = oncrpc::Channel::open(ai);
                auto client = std::make_shared<oncrpc::SysClient>(
                    nfs4::NFS4_PROGRAM, nfs4::NFS_V4);
                auto ds = make_shared<nfs4::NfsDataStore>(
                    chan, client, clock, "distfsck");
                servers[dsid] = ds;
                break;
            }
            catch (system_error& e) {
                continue;
            }
        }
        iterator->next();
    }
    LOG(INFO) << servers;

    // Check that data locations are consistent with the piece table.
    DataKeyType datastart(1, 0), dataend(~0ul, 0);
    iterator = dataNS->iterator(datastart, dataend);
    while (iterator->valid()) {
        PieceData key(iterator->key());
        auto fileid = key.fileid();
        auto offset = key.offset();
        auto size = key.size();

        //cerr << "checking {" << fileid << "," << offset << "}" << endl;
        auto val = iterator->value();
        oncrpc::XdrMemory xm(val->data(), val->size());
        PieceLocation loc;
        xdr(loc, static_cast<oncrpc::XdrSource*>(&xm));

        for (auto entry: loc) {
            //cerr << "checking device: " << entry.device << ", index: " << entry.index << endl;
            try {
                PieceData val(
                    piecesNS->get(DoubleKeyType(entry.device, entry.index)));
                if (val.fileid() != fileid || val.offset() != offset) {
                    cerr << "fileid: " << fileid
                         << ", offset: " << offset
                         << " inconsistent with pieces table" << endl;
                }
                auto ds = servers[entry.device];
                Credential cred{0, 0, {}, true};
                if (ds) {
                    auto file = ds->findPiece(
                        cred, PieceId{fileid, offset, size});
                }
                else {
                    cerr << "no connection for device: "
                         << entry.device << endl;
                }
            }
            catch (system_error&) {
                cerr << "fileid: " << fileid
                     << ", offset: " << offset
                     << " expected pieces table entry for"
                     << " device: " << entry.device
                     << ", index: " << entry.index << endl;
            }
        }

        iterator->next();
    }
}
