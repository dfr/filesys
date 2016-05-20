// -*- c++ -*-
#pragma once

#include <map>

#include "filesys/objfs/objfsck.h"
#include "filesys/distfs/distfsproto.h"

namespace filesys {
namespace distfs {

class DistfsCheck: public objfs::ObjfsCheck
{
public:
    DistfsCheck(std::unique_ptr<keyval::Database>&& db)
        : ObjfsCheck(std::move(db))
    {
    }

    void check();
};

}
}
