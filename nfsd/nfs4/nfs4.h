/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#pragma once

namespace oncrpc {
class ServiceRegistry;
}

namespace nfsd {

class ThreadPool;

namespace nfs4 {

void init(
    std::shared_ptr<oncrpc::ServiceRegistry> svcreg,
    std::shared_ptr<ThreadPool> threadpool,
    const std::vector<int>& sec,
    const std::vector<oncrpc::AddressInfo>& addrs);

}

}
