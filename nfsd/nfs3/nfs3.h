/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

namespace oncrpc {
class ServiceRegistry;
}

namespace nfsd {

class ThreadPool;

namespace nfs3 {

void init(
    std::shared_ptr<oncrpc::ServiceRegistry> svcreg,
    std::shared_ptr<ThreadPool> threadpool,
    const std::vector<int>& sec,
    const std::vector<oncrpc::AddressInfo>& addrs);

}

}
