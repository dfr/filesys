#pragma once

#include <map>

#include "filesys/objfs/objfsproto.h"
#include "filesys/objfs/objfskey.h"

namespace filesys {
namespace objfs {

class Database;

class ObjfsCheck
{
public:
    ObjfsCheck(std::unique_ptr<Database>&& db)
        : db_(std::move(db))
    {
    }

    void check();

private:
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

    std::unique_ptr<Database> db_;
    std::map<std::uint64_t, state> files_;
};

}
}
