/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include <memory>
#include <string>

namespace filesys {
namespace nfs4 {

class IIdMapper
{
public:
    virtual int toUid(const std::string& s) = 0;
    virtual std::string fromUid(int uid) = 0;
    virtual int toGid(const std::string& s) = 0;
    virtual std::string fromGid(int uid) = 0;
};

std::shared_ptr<IIdMapper> LocalIdMapper();

}
}
