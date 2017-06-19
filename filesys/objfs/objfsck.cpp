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

#include "objfsck.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace keyval;
using namespace std;

void ObjfsCheck::check(bool checkData)
{
    auto defaultNS = db_->getNamespace("default");
    auto directoriesNS = db_->getNamespace("directories");
    auto dataNS = db_->getNamespace("data");
    ObjFilesystemMeta fsmeta;
    try {
        auto buf = defaultNS->get(KeyType(0));
        oncrpc::XdrMemory xm(buf->data(), buf->size());
        xdr(fsmeta, static_cast<oncrpc::XdrSource*>(&xm));
        if (fsmeta.vers != 1) {
            LOG(ERROR) << "unexpected filesystem metadata version: "
                << fsmeta.vers << ", expected: " << 1;
            throw system_error(EACCES, system_category());
        }
    }
    catch (oncrpc::XdrError&) {
        LOG(ERROR) << "error decoding filesystem metadata";
        throw system_error(EACCES, system_category());
    }
    catch (system_error&) {
        return;
    }

    KeyType start(1), end(~0ul);
    auto iterator = defaultNS->iterator(start, end);
    while (iterator->valid()) {
        KeyType k(iterator->key());
        auto id = k.id();
        ObjFileMeta meta;
        auto val = iterator->value();
        oncrpc::XdrMemory xm(val->data(), val->size());
        xdr(meta, static_cast<oncrpc::XdrSource*>(&xm));

        assert(meta.fileid == id);
        files_[id] = state{
            meta.blockSize, meta.attr.type, meta.attr.nlink, 0, 0};
        iterator->next();
    }
    iterator.reset();

    DirectoryKeyType dirstart(1, ""), dirend(~0ul, "");
    iterator = directoriesNS->iterator(dirstart, dirend);
    while (iterator->valid()) {
        DirectoryKeyType k(iterator->key());

        DirectoryEntry entry;
        DirectoryKeyType key(iterator->key());
        auto val = iterator->value();
        oncrpc::XdrMemory xm(val->data(), val->size());
        xdr(entry, static_cast<oncrpc::XdrSource*>(&xm));

        if (files_.find(entry.fileid) == files_.end()) {
            cerr << key.fileid() << ": entry " << key.name()
                 << " references unknown fileid: " << entry.fileid << endl;
        }

        auto& f = files_[entry.fileid];
        f.refs++;
        auto name = k.name();
        if (name != "." && name != "..") {
            f.parent = k.fileid();
            f.name = name;
        }

        iterator->next();
    }
    iterator.reset();

    if (checkData) {
        DataKeyType datastart(1, 0), dataend(~0ul, 0);
        iterator = dataNS->iterator(datastart, dataend);
        uint64_t lastOffset = 0;
        uint64_t lastFileid = 0;
        while (iterator->valid()) {
            DataKeyType key(iterator->key());
            auto fileid = key.fileid();
            auto offset = key.offset();
            auto blockSize = files_[fileid].blockSize;
            if (lastFileid != fileid) {
                lastFileid = fileid;
                lastOffset = 0;
            }
            if (files_.find(key.fileid()) == files_.end()) {
                cerr << "Orphan data block for unknown fileid: "
                     << key.fileid() << endl;
            }
            if (offset % blockSize) {
                cerr << "fileid: " << fileid
                     << " unaligned block offset " << offset << endl;
            }
            if (offset < lastOffset) {
                cerr << "fileid: " << fileid
                     << " disordered offset " << offset << endl;
            }
            lastOffset = offset + blockSize;
            iterator->next();
        }
    }

    for (auto& i: files_) {
        // Verify that nlink is consistent with directory contents
        if (i.second.nlink != i.second.refs) {
            cerr << i.first << " (" << pathname(i.first) << "): "
                 << "nlink is " << i.second.nlink
                 << ", expected " << i.second.refs << endl;
        }

        // Directories (apart from the root) must have a parent
        if (i.second.type == PT_DIR && i.first != 1) {
            if (i.second.parent == 0)
                cerr << "Directory " << i.first << " has no parent" << endl;
        }
    }
}
