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

// -*- c++ -*-
#pragma once

#include <map>

#include <keyval/keyval.h>

#include "filesys/objfs/objfsproto.h"
#include "filesys/objfs/objfskey.h"

namespace filesys {
namespace objfs {

class ObjfsCheck
{
public:
    ObjfsCheck(std::shared_ptr<keyval::Database> db)
        : db_(db)
    {
    }

    void check(bool checkData);

protected:
    struct state {
        std::uint32_t blockSize;
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

    std::shared_ptr<keyval::Database> db_;
    std::uint32_t blockSize_;
    std::map<std::uint64_t, state> files_;
};

}
}
