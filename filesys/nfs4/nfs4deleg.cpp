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
      delegation_(move(delegation)),
      valid_(true)
{
    if (delegation_.delegation_type == OPEN_DELEGATE_READ)
        stateid_ = delegation_.read().stateid;
    else
        stateid_ = delegation_.write().stateid;
}

NfsDelegation::~NfsDelegation()
{
    if (valid_) {
        // Push any locally cached writes to the server. Don't force the
        // commit here since the file may stay open.
        file_->clearDelegation();
        file_->flush(stateid_, false);

        try {
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
        catch (system_error& e) {
            // Ignore system errors, e.g. if the server has expired our
            // client
        }
    }
}
