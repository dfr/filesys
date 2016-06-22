/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <cassert>
#include <system_error>

#include "nfs4fs.h"

using namespace filesys;
using namespace filesys::nfs4;
using namespace std;

NfsDelegation::NfsDelegation(
    shared_ptr<NfsProto> proto,
    shared_ptr<NfsFile> file,
    shared_ptr<NfsOpenFile> of,
    open_delegation4&& delegation)
    : proto_(proto),
      file_(file),
      open_(of),
      delegation_(move(delegation))
{
    if (delegation_.delegation_type == OPEN_DELEGATE_READ)
        stateid_ = delegation_.read().stateid;
    else
        stateid_ = delegation_.write().stateid;
}

NfsDelegation::~NfsDelegation()
{
    // Push any locally cached writes to the server. Don't force the
    // commit here since the file may stay open.
    file_->clearDelegation();
    file_->flush(stateid_, false);

    proto_->compound(
        "delegreturn",
        [this](auto& enc) {
            enc.putfh(open_->fh());
            enc.delegreturn(stateid_);
        },
        [](auto& dec) {
            dec.putfh();
            dec.delegreturn();
        });
}
