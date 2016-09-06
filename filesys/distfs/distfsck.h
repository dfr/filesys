/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

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
    DistfsCheck(std::shared_ptr<keyval::Database> db)
        : ObjfsCheck(db)
    {
    }

    void check();
};

}
}
