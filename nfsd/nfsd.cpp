/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <rpc++/cred.h>
#include <rpc++/rest.h>
#include <rpc++/server.h>
#include <rpc++/urlparser.h>
#include <keyval/keyval.h>
#include <filesys/filesys.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "threadpool.h"
#include "version.h"

#include "nfs3/nfs3.h"
#include "nfs4/nfs4.h"

using namespace filesys;
using namespace nfsd;
using namespace oncrpc;
using namespace std;

namespace {

#include "nfsd/ui/ui.h"

}

DEFINE_int32(port, 2049, "port to listen for connections");
DEFINE_int32(uiport, 80, "port to listen for UI connections");
DEFINE_int32(iosize, 65536, "maximum size for read or write requests");
DEFINE_int32(idle_timeout, 30, "idle timeout in seconds");
DEFINE_int32(grace_time, 120, "NFSv4 grace period time in seconds");
DEFINE_int32(lease_time, 120, "NFSv4 lease time in seconds");
DEFINE_string(sec, "sys", "Acceptable authentication flavors");
DEFINE_string(realm, "", "Local krb5 realm name");
DEFINE_int32(threads, 0, "Number of worker threads");
DEFINE_string(listen, "[::],0.0.0.0", "Addresses to listen for connections");
DEFINE_bool(daemon, false, "Run the server as a background task");
DEFINE_string(fsid, "", "Override file system identifier for new filesystems");
DEFINE_string(allow, "", "Networks allowed to send requests");
DEFINE_string(deny, "", "Networks not allowed to send requests");

namespace {

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

class ExportVersion: public oncrpc::RestHandler
{
public:
    bool get(
        std::shared_ptr<oncrpc::RestRequest> req,
        std::unique_ptr<oncrpc::RestEncoder>&& res) override
    {
        auto obj = res->object();
        obj->field("commit")->string(nfsd::version::commit);
        obj->field("date")->number(long(nfsd::version::date));
        return true;
    }
};

static inline char toHexChar(int digit)
{
    static char hex[] = {
        '0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'
    };
    return hex[digit];
}

static inline std::string toHex(uint64_t id)
{
    std::string res;
    res.resize(16);
    for (int i = 0; i < 16; i++) {
        res[i] = toHexChar(id >> 60);
        id <<= 4;
    }
    return res;
}

class ExportFsattr: public oncrpc::RestHandler
{
public:
    ExportFsattr(std::shared_ptr<Filesystem> fs)
        : fs_(fs)
    {
    }

    bool get(
        std::shared_ptr<oncrpc::RestRequest> req,
        std::unique_ptr<oncrpc::RestEncoder>&& res) override
    {
        Credential cred(0, 0, {}, true);
        auto stat = fs_->root()->fsstat(cred);
        auto obj = res->object();

        auto stats = obj->field("stats")->object();
        stats->field("totalSpace")->number(long(stat->totalSpace()));
        stats->field("freeSpace")->number(long(stat->freeSpace()));
        stats->field("availSpace")->number(long(stat->availSpace()));
        stats->field("totalFiles")->number(long(stat->totalFiles()));
        stats->field("freeFiles")->number(long(stat->freeFiles()));
        stats->field("availFiles")->number(long(stat->availFiles()));
        stats->field("repairQueueSize")->number(stat->repairQueueSize());
        stats.reset();

        auto devs = obj->field("devices")->array();
        std::uint64_t gen;
        auto list = fs_->devices(gen);
        for (auto devp: list) {
            static const char* stateNames[] = {
                "unknown", "restoring", "missing", "dead", "healthy"
            };
            auto dev = devs->element()->object();
            dev->field("id")->string(toHex(devp->id()));
            dev->field("state")->string(stateNames[devp->state()]);
            auto addrs = dev->field("addresses")->array();
            for (auto& ai: devp->addresses()) {
                auto entry = addrs->element()->object();
                entry->field("netid")->string(ai.netid());
                entry->field("uaddr")->string(ai.uaddr());
                entry->field("host")->string(ai.host());
                entry->field("port")->number(ai.port());
            }
            addrs.reset();

            auto adminAddrs = dev->field("adminAddresses")->array();
            for (auto& ai: devp->adminAddresses()) {
                auto entry = adminAddrs->element()->object();
                entry->field("netid")->string(ai.netid());
                entry->field("uaddr")->string(ai.uaddr());
                entry->field("host")->string(ai.host());
                entry->field("port")->number(ai.port());
            }
            adminAddrs.reset();

            dev.reset();
        }
        devs.reset();

        auto db = fs_->database();
        auto replicas = obj->field("replicas")->array();
        if (db && db->isReplicated()) {
            // The first entry is always the master replica
            bool isMaster = true;
            auto info = db->getReplicas();
            for (auto& entry: info) {
                static const char* stateNames[] = {
                    "dead", "healthy", "recovering", "unknown"
                };
                auto replica = replicas->element()->object();
                replica->field("isMaster")->boolean(isMaster);
                replica->field("state")->string(stateNames[entry.state]);
                auto addrs = replica->field("addresses")->array();
                if (entry.appdata.size() > 0) {
                    vector<string> uaddrs;
                    oncrpc::XdrMemory xm(
                        entry.appdata.data(), entry.appdata.size());
                    xdr(uaddrs, static_cast<oncrpc::XdrSource*>(&xm));
                    for (auto& u: uaddrs) {
                        // For the user interface, we need the admin port,
                        // not the data port. We assume that all replicas
                        // use the same admin port.
                        auto ai = oncrpc::AddressInfo::fromUaddr(u, "tcp");
                        auto entry = addrs->element()->object();
                        entry->field("netid")->string(ai.netid());
                        entry->field("uaddr")->string(ai.uaddr());
                        entry->field("host")->string(ai.host());
                        entry->field("port")->number(FLAGS_uiport);
                    }
                }

                addrs.reset();
                replica.reset();
                isMaster = false;
            }
        }
        replicas.reset();

        obj.reset();
        return true;
    }

private:
    std::shared_ptr<Filesystem> fs_;
};

class ExportDbstats: public oncrpc::RestHandler
{
public:
    ExportDbstats(std::shared_ptr<keyval::Database> db)
        : db_(db)
    {
    }

    bool get(
        std::shared_ptr<oncrpc::RestRequest> req,
        std::unique_ptr<oncrpc::RestEncoder>&& res) override
    {
        return db_.lock()->get(req, std::move(res));
    }

private:
    std::weak_ptr<keyval::Database> db_;
};

class QuitHook: public oncrpc::RestHandler
{
public:
    QuitHook(shared_ptr<SocketManager> sockman)
        : sockman_(sockman)
    {
    }

    bool post(
        std::shared_ptr<oncrpc::RestRequest> req,
        std::unique_ptr<oncrpc::RestEncoder>&& res) override
    {
        if (req->body() == "true") {
            sockman_.lock()->stop();
        }
        return true;
    }

private:
    std::weak_ptr<SocketManager> sockman_;
};

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

    if (FLAGS_daemon)
        ::daemon(true, true);

    // Access the root node to make sure it is created (if necessary)
    auto fs = fac->mount(argv[1]);
    fs->root();
    fsman.mount("/", fs);

    shared_ptr<Filter> filter;
    if (FLAGS_allow.size() > 0 || FLAGS_deny.size() > 0) {
        filter = make_shared<Filter>();
        try {
            auto s = FLAGS_allow;
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
                filter->allow(Network(addr));
            }
            s = FLAGS_deny;
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
                filter->deny(Network(addr));
            }
        }
        catch (runtime_error& e) {
            cerr << e.what() << endl;
            return 1;
        }
    }

    auto svcreg = make_shared<ServiceRegistry>();
    svcreg->setFilter(filter);
    if (FLAGS_realm.size() > 0)
        svcreg->mapCredentials(FLAGS_realm, make_shared<LocalCredMapper>());

    auto restreg = make_shared<RestRegistry>();
    restreg->setFilter(filter);
    registerUiContent(restreg);
    restreg->add("/version", true, make_shared<ExportVersion>());
    restreg->add("/fsattr", true, make_shared<ExportFsattr>(fs));
    auto db = fs->database();
    if (db)
        restreg->add("/dbstats", true, make_shared<ExportDbstats>(db));
    auto sockman = make_shared<SocketManager>();
    restreg->add("/quit", true, make_shared<QuitHook>(sockman));

    vector<AddressInfo> addrs;
    vector<AddressInfo> uiaddrs;
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

        url = "tcp://" + addr + ":" + to_string(FLAGS_uiport);
        for (auto& ai: getAddressInfo(url))
            uiaddrs.push_back(ai);
    }

    vector<AddressInfo> boundAddrs;
    vector<AddressInfo> boundAdminAddrs;
    for (auto& ai: addrs) {
        try {
            int fd = socket(ai.family, ai.socktype, ai.protocol);
            if (fd < 0)
                throw system_error(errno, system_category());
            int one = 1;
            ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
            auto sock = make_shared<ListenSocket>(fd, svcreg, nullptr);
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

    sockman->setIdleTimeout(std::chrono::seconds(FLAGS_idle_timeout));
    for (auto& ai: uiaddrs) {
        try {
            int fd = socket(ai.family, ai.socktype, ai.protocol);
            if (fd < 0)
                throw system_error(errno, system_category());
            int one = 1;
            ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
            auto sock = make_shared<ListenSocket>(fd, nullptr, restreg);
            sock->bind(ai.addr);
            sock->listen();
            sock->setBufferSize(FLAGS_iosize + 512);
            sockman->add(sock);
            boundAdminAddrs.push_back(ai);
        }
        catch (runtime_error& e) {
            cout << "nfsd: " << e.what() << endl;
            exit(1);
        }
    }

    auto threadpool = make_shared<ThreadPool>(FLAGS_threads);

    nfs3::init(svcreg, restreg, threadpool, sec, boundAddrs, fs);
    nfs4::init(sockman, svcreg, restreg, threadpool, sec, boundAddrs, fs);

    auto ds = dynamic_pointer_cast<DataStore>(fs);
    if (ds) {
        ds->reportStatus(sockman, argv[1], boundAddrs, boundAdminAddrs);
    }

    sockman->run();

    threadpool.reset();
    nfs3::shutdown();
    nfs4::shutdown();
    fsman.clear();
    sockman.reset();
    svcreg.reset();
    restreg.reset();
    fs.reset();
}
