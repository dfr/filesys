/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <keyval/keyval.h>

#include "keyval/mem/mem.h"
#include "keyval/rocks/rocks.h"

using namespace keyval;

std::unique_ptr<Database> keyval::make_memdb()
{
    return std::make_unique<keyval::memory::MemoryDatabase>();
}

std::unique_ptr<Database> keyval::make_rocksdb(const std::string& filename)
{
    return std::make_unique<keyval::rocks::RocksDatabase>(filename);
}
