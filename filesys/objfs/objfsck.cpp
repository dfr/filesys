#include <cassert>
#include <iostream>
#include <system_error>

#include <rpc++/xdr.h>
#include <glog/logging.h>

#include "objfsck.h"
#include "filesys/objfs/rocksdbi.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace std;

void ObjfsCheck::check()
{
    auto namespaces = db_->open({"default", "directories", "data"});
    auto defaultNS = namespaces[0];
    auto directoriesNS = namespaces[1];
    auto dataNS = namespaces[2];
    ObjFilesystemMeta fsmeta;
    try {
        auto buf = db_->get(defaultNS, KeyType(0));
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
    auto iterator = db_->iterator(defaultNS);
    iterator->seek(start);
    while (iterator->valid(end)) {
        KeyType k(iterator->key());
        auto id = k.fileid();
        ObjFileMeta meta;
        auto val = iterator->value();
        oncrpc::XdrMemory xm(val->data(), val->size());
        xdr(meta, static_cast<oncrpc::XdrSource*>(&xm));

        assert(meta.fileid == id);
        files_[id] = state{meta.attr.type, meta.attr.nlink, 0, 0};
        iterator->next();
    }
    iterator.reset();

    DirectoryKeyType dirstart(1, ""), dirend(~0ul, "");
    iterator = db_->iterator(directoriesNS);
    iterator->seek(dirstart);
    while (iterator->valid(dirend)) {
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

    DataKeyType datastart(1, 0), dataend(~0ul, 0);
    iterator = db_->iterator(dataNS);
    iterator->seek(datastart);
    uint64_t lastOffset = 0;
    uint64_t lastFileid = 0;
    while (iterator->valid(dataend)) {
        DataKeyType key(iterator->key());
        auto fileid = key.fileid();
        auto offset = key.offset();
        if (lastFileid != fileid) {
            lastFileid = fileid;
            lastOffset = 0;
        }
        if (files_.find(key.fileid()) == files_.end()) {
            cerr << "Orphan data block for unknown fileid: "
                 << key.fileid() << endl;
        }
        if (offset % fsmeta.blockSize) {
            cerr << "fileid: " << fileid
                 << " unaligned block offset " << offset << endl;
        }
        if (offset < lastOffset) {
            cerr << "fileid: " << fileid
                 << " disordered offset " << offset << endl;
        }
        lastOffset = offset + fsmeta.blockSize;
        iterator->next();
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