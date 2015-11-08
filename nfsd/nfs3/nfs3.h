#pragma once

namespace oncrpc {
class ServiceRegistry;
}

namespace nfsd {
namespace nfs3 {

void init(std::shared_ptr<oncrpc::ServiceRegistry> svcreg);

}
}
