#include <cassert>
#include <iostream>
#include <system_error>

#include <rpc++/xdr.h>
#include <glog/logging.h>

#include "filesys/objfs/objfsck.h"
#include "filesys/objfs/rocksdbi.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace std;

[[noreturn]] void usage()
{
    cerr << "usage: objfsck <directory>" << endl;
    exit(1);
}

int main(int argc, char** argv)
{
    if (argc != 2)
        usage();

    ObjfsCheck(make_unique<RocksDatabase>(argv[1])).check();
}