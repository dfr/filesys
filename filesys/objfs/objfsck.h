// -*- c++ -*-
#pragma once

#include <map>

#include <kv++/keyval.h>

#include "filesys/objfs/objfsproto.h"
#include "filesys/objfs/objfskey.h"

namespace filesys {
namespace objfs {

class ObjfsCheck
{
public:
    ObjfsCheck(std::unique_ptr<keyval::Database>&& db)
        : db_(std::move(db))
    {
    }

    void check();

protected:
    struct state {
        PosixType type;
        std::uint32_t nlink;
        std::uint32_t refs;
        std::uint64_t parent;
        std::string name;
    };

    std::string pathname(uint64_t fileid)
    {
        auto& f = files_[fileid];
        if (f.parent)
            return pathname(f.parent) + "/" + f.name;
        else
            return f.name;
    }

    std::unique_ptr<keyval::Database> db_;
    std::uint32_t blockSize_;
    std::map<std::uint64_t, state> files_;
};

}
}
