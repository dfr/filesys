/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#pragma once

namespace filesys {
class Filesystem;
}

namespace oncrpc {
class ServiceRegistry;
}

namespace nfsd {

class ThreadPool;

namespace nfs4 {

void init(
    std::shared_ptr<oncrpc::SocketManager> sockman,
    std::shared_ptr<oncrpc::ServiceRegistry> svcreg,
    std::shared_ptr<oncrpc::RestRegistry> restreg,
    std::shared_ptr<ThreadPool> threadpool,
    const std::vector<int>& sec,
    const std::vector<oncrpc::AddressInfo>& addrs,
    std::shared_ptr<filesys::Filesystem> fs);

void shutdown();

}

}
