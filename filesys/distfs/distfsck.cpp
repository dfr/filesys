/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
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
    iterator = dataNS->iterator();
    iterator->seek(datastart);
    while (iterator->valid(dataend)) {
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
