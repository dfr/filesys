#include <chrono>
#include <iostream>
#include <sstream>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <rpc++/cred.h>
#include <rpc++/server.h>
#include <fs++/filesys.h>
#include <fs++/urlparser.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "threadpool.h"

#include "nfs3/nfs3.h"
#include "nfs4/nfs4.h"

using namespace filesys;
using namespace nfsd;
using namespace oncrpc;
using namespace std;

DEFINE_int32(port, 2049, "port to listen for connections");
DEFINE_int32(iosize, 65536, "maximum size for read or write requests");
DEFINE_int32(idle_timeout, 30, "idle timeout in seconds");
DEFINE_int32(grace_time, 120, "NFSv4 grace period time in seconds");
DEFINE_int32(lease_time, 120, "NFSv4 lease time in seconds");
DEFINE_string(sec, "sys", "Acceptable authentication flavors");
DEFINE_string(realm, "", "Local krb5 realm name");
DEFINE_int32(threads, 0, "Number of worker threads");
DEFINE_string(listen, "[::],0.0.0.0", "Addresses to listen for connections");
DEFINE_string(mds, "", "URL to contact metadata server");

static map<string, int> flavors {
    { "none", AUTH_NONE },
    { "sys", AUTH_SYS },
    { "krb5", RPCSEC_GSS_KRB5 },
    { "krb5i", RPCSEC_GSS_KRB5I },
    { "krb5p", RPCSEC_GSS_KRB5P },
};

static vector<int> parseSec()
{
    stringstream ss(FLAGS_sec);
    string flavor;
    vector<int> res;

    while (getline(ss, flavor, ',')) {
        auto i = flavors.find(flavor);
        if (i == flavors.end()) {
            cerr << "nfsd: unknown authentication flavor: " << flavor << endl;
            exit(1);
        }
        res.push_back(i->second);
    }
    return res;
}

int main(int argc, char** argv)
{
    gflags::SetUsageMessage("usage: nfsd <url>");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 2) {
        gflags::ShowUsageWithFlagsRestrict(argv[0], "nfsd.cpp");
        return 1;
    }
    google::InitGoogleLogging(argv[0]);

    auto sec = parseSec();

    auto& fsman = FilesystemManager::instance();

    UrlParser p(argv[1]);
    auto fac = fsman.find(p.scheme);
    if (!fac) {
        cerr << argv[1] << ": unsupported url scheme" << endl;
        return 1;
    }
    auto mnt = fac->mount(&fsman, argv[1]);

    auto svcreg = make_shared<ServiceRegistry>();
    if (FLAGS_realm.size() > 0)
        svcreg->mapCredentials(FLAGS_realm, make_shared<LocalCredMapper>());

    auto sockman = make_shared<SocketManager>();
    sockman->setIdleTimeout(std::chrono::seconds(FLAGS_idle_timeout));

    vector<AddressInfo> addrs;
    auto s = FLAGS_listen;
    while (s.size() > 0) {
        auto i = s.find(',');
        string addr;
        if (i == string::npos) {
            addr = s;
            s = "";
        }
        else {
            addr = s.substr(0, i);
            s = s.substr(i + 1);
        }
        auto url = "tcp://" + addr + ":" + to_string(FLAGS_port);
        for (auto& ai: getAddressInfo(url))
            addrs.push_back(ai);
    }

    vector<AddressInfo> boundAddrs;
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
            boundAddrs.push_back(ai);
        }
        catch (runtime_error& e) {
            cout << "nfsd: " << e.what() << endl;
            exit(1);
        }
    }

    auto threadpool = make_shared<ThreadPool>(FLAGS_threads);

    nfs3::init(svcreg, threadpool, sec, boundAddrs);
    nfs4::init(svcreg, threadpool, sec, boundAddrs);

    if (FLAGS_mds.size() > 0) {
        auto ds = dynamic_pointer_cast<DataStore>(mnt.first);
        if (ds) {
            ds->reportStatus(sockman, FLAGS_mds, boundAddrs);
        }
    }

    sockman->run();
}
