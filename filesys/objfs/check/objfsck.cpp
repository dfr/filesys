#include <cassert>
#include <iostream>
#include <system_error>

#include <rpc++/xdr.h>
#include <glog/logging.h>

#include "filesys/objfs/objfsproto.h"
#include "filesys/objfs/objfskey.h"
#include "filesys/objfs/rocksdbi.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace std;

struct state {
    PosixType type;
    uint32_t nlink;
    uint32_t refs;
    uint64_t parent;
    string name;
};
map<uint64_t, state> files;

string pathname(uint64_t fileid)
{
    auto& f = files[fileid];
    if (f.parent)
        return pathname(f.parent) + "/" + f.name;
    else
        return f.name;
}

void check(const std::string& filename)
{
    auto db = make_unique<RocksDatabase>(filename);
    auto namespaces = db->open({"default", "directories", "data"});
    auto defaultNS = namespaces[0];
    auto directoriesNS = namespaces[1];
    auto dataNS = namespaces[2];
    ObjFilesystemMeta fsmeta;
    try {
        auto buf = db->get(defaultNS, KeyType(0));
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
    auto iterator = db->iterator(defaultNS);
    iterator->seek(start);
    while (iterator->valid(end)) {
        KeyType k(iterator->key());
        auto id = k.fileid();
        ObjFileMeta meta;
        auto val = iterator->value();
        oncrpc::XdrMemory xm(val->data(), val->size());
        xdr(meta, static_cast<oncrpc::XdrSource*>(&xm));

        assert(meta.fileid == id);
        files[id] = state{meta.attr.type, meta.attr.nlink, 0, 0};
        iterator->next();
    }
    iterator.reset();

    DirectoryKeyType dirstart(1, ""), dirend(~0ul, "");
    iterator = db->iterator(directoriesNS);
    iterator->seek(dirstart);
    while (iterator->valid(dirend)) {
        DirectoryKeyType k(iterator->key());

        DirectoryEntry entry;
        DirectoryKeyType key(iterator->key());
        auto val = iterator->value();
        oncrpc::XdrMemory xm(val->data(), val->size());
        xdr(entry, static_cast<oncrpc::XdrSource*>(&xm));

        if (files.find(entry.fileid) == files.end()) {
            cerr << key.fileid() << ": entry " << key.name()
                 << " references unknown fileid: " << entry.fileid << endl;
        }

        auto& f = files[entry.fileid];
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
    iterator = db->iterator(dataNS);
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
        if (files.find(key.fileid()) == files.end()) {
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

    for (auto& i: files) {
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

[[noreturn]] void usage()
{
    cerr << "usage: objfsck <directory>" << endl;
    exit(1);
}

int main(int argc, char** argv)
{
    if (argc != 2)
        usage();

    check(argv[1]);
}
