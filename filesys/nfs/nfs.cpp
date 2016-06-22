/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <cassert>
#include <iomanip>
#include <random>
#include <system_error>
#include <unistd.h>

#include <glog/logging.h>
#include <fs++/urlparser.h>

#include "nfs.h"
#include "filesys/nfs3/nfs3fs.h"
#include "filesys/nfs4/nfs4fs.h"

using namespace filesys;
using namespace filesys::nfs;

std::shared_ptr<Filesystem>
NfsFilesystemFactory::mount(const std::string& url)
{
    using namespace oncrpc;

    UrlParser p(url);
    int nfsvers;
    auto it = p.query.find("version");
    if (it == p.query.end()) {
        nfsvers = 3;
        auto chan = Channel::open(url, "tcp");
        // Figure out what protocol versions the server supports
        constexpr int NFS_PROGRAM = 100003;
        auto client = std::make_shared<SysClient>(NFS_PROGRAM, 0);
        try {
            // This should throw a VersionMismatch error - if it doesn't,
            // the server is broken.
            chan->call(client.get(), 0, [](auto){}, [](auto){});
            LOG(ERROR) << "Expected version mismatch error from server"
                       << " - assuming NFSv3";
        }
        catch (VersionMismatch& e) {
            nfsvers = e.maxver();
            if (nfsvers > 4)
                nfsvers = 4;
        }
    }
    else {
        nfsvers = std::stoi(it->second);
    }

    VLOG(1) << url << ": using NFSv" << nfsvers;

    switch (nfsvers) {
    case 3:
        return filesys::nfs3::NfsFilesystemFactory().mount(url);

    case 4:
        return filesys::nfs4::NfsFilesystemFactory().mount(url);

    default:
        LOG(ERROR) << "Unsupported NFS version " << nfsvers;
        abort();
    }
}

void filesys::nfs::init(FilesystemManager* fsman)
{
    UrlParser::addHostbasedScheme("nfs");
    fsman->add(std::make_shared<NfsFilesystemFactory>());
}
