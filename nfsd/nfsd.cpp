#include <chrono>
#include <iostream>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <rpc++/server.h>
#include <fs++/filesys.h>
#include <fs++/urlparser.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "nfs3/nfs3.h"

using namespace filesys;
using namespace nfsd;
using namespace oncrpc;
using namespace std;

DEFINE_int32(port, 2049, "port to listen for connections");
DEFINE_int32(iosize, 65536, "maximum size for read or write requests");
DEFINE_int32(idle_timeout, 30, "idle timeout in seconds");

int main(int argc, char** argv)
{
    gflags::SetUsageMessage("usage: nfsd <url>");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 2) {
        gflags::ShowUsageWithFlagsRestrict(argv[0], "nfsd.cpp");
        return 1;
    }
    google::InitGoogleLogging(argv[0]);

    auto& fsman = FilesystemManager::instance();

    UrlParser p(argv[1]);
    auto fac = fsman.find(p.scheme);
    if (!fac) {
        cerr << argv[1] << ": unsupported url scheme" << endl;
        return 1;
    }
    auto mnt = fac->mount(&fsman, argv[1]);

    auto svcreg = make_shared<ServiceRegistry>();
    nfs3::init(svcreg);

    auto sockman = make_shared<SocketManager>();
    sockman->setIdleTimeout(std::chrono::seconds(FLAGS_idle_timeout));
    auto addrs = getAddressInfo("tcp://[::]:" + to_string(FLAGS_port), "tcp");
    for (auto& ai: addrs) {
        try {
            int fd = socket(ai.family, ai.socktype, ai.protocol);
            if (fd < 0)
                throw system_error(errno, system_category());
            int one = 1;
            ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
            auto sock = make_shared<ListenSocket>(fd, svcreg);
            sock->bind(ai.addr);
            sock->listen();
            sock->setBufferSize(FLAGS_iosize + 512);
            sockman->add(sock);
        }
        catch (runtime_error& e) {
            cout << "nfsd: " << e.what() << endl;
            exit(1);
        }
    }

    sockman->run();
}
