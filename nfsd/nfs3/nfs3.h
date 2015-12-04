#pragma once

namespace oncrpc {
class ServiceRegistry;
}

namespace nfsd {
namespace nfs3 {

void init(
    std::shared_ptr<oncrpc::ServiceRegistry> svcreg,
    const std::vector<int>& sec,
    const std::vector<oncrpc::AddressInfo>& addrs);

}
}
