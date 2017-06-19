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

#include <cassert>
#include <iomanip>
#include <random>
#include <system_error>
#include <unistd.h>

#include <glog/logging.h>
#include <rpc++/urlparser.h>

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
    oncrpc::UrlParser::addHostbasedScheme("nfs");
    fsman->add(std::make_shared<NfsFilesystemFactory>());
}
