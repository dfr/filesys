/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <cassert>
#include <iostream>
#include <system_error>

#include <rpc++/xdr.h>
#include <glog/logging.h>

#include "filesys/distfs/distfsck.h"

using namespace filesys;
using namespace filesys::distfs;
using namespace std;

[[noreturn]] void usage()
{
    cerr << "usage: distfsck <directory>" << endl;
    exit(1);
}

int main(int argc, char** argv)
{
    if (argc != 2)
        usage();

    DistfsCheck(keyval::make_rocksdb(argv[1])).check();
}
