#include <cassert>
#include <iomanip>
#include <random>
#include <system_error>
#include <unistd.h>

#include <fs++/urlparser.h>

#include "nfs4fs.h"

using namespace filesys;
using namespace filesys::nfs4;
using namespace std;

DEFINE_string(clientowner, "", "Override default client owner");

static random_device rnd;

NfsFilesystem::NfsFilesystem(
    shared_ptr<oncrpc::Channel> chan,
    shared_ptr<oncrpc::Client> client,
    shared_ptr<detail::Clock> clock,
    shared_ptr<IIdMapper> idmapper)
    : chan_(chan),
      client_(client),
      clock_(clock),
      idmapper_(idmapper),
      tag_("fscli"),
      sockman_(make_shared<oncrpc::SocketManager>()),
      svcreg_(chan->serviceRegistry()),
      cbsvc_(this)
{
    // Only allow a fraction of the file cache to have active delegations
    delegations_.setSizeLimit(cache_.sizeLimit() / 8);

    // Handle callbacks on a new thread
    if (!svcreg_) {
        svcreg_ = make_shared<oncrpc::ServiceRegistry>();
        chan->setServiceRegistry(svcreg_);
    }
    cbsvc_.bind(svcreg_);
    cbthread_ = thread([this, chan](){ handleCallbacks(); });

    // First we need to create a new session
    char hostname[256];
    if (::gethostname(hostname, sizeof(hostname)) < 0)
        throw system_error(errno, system_category());

    for (auto& b: clientOwner_.co_verifier)
        b = rnd();
    string s;
    if (FLAGS_clientowner.size() > 0) {
        s = FLAGS_clientowner;
    }
    else {
        ostringstream ss;
        ss << "unfscl" << ::getpgrp() << "@" << hostname;
        s = ss.str();
    }
    clientOwner_.co_ownerid.resize(s.size());
    copy_n(s.data(), s.size(), clientOwner_.co_ownerid.data());
    VLOG(1) << "Using client owner " << clientOwner_;

    connect();
}

NfsFilesystem::NfsFilesystem(
    shared_ptr<oncrpc::Channel> chan,
    shared_ptr<oncrpc::Client> client,
    shared_ptr<detail::Clock> clock)
    : NfsFilesystem(chan, client, clock, LocalIdMapper())
{
}

NfsFilesystem::~NfsFilesystem()
{
}

shared_ptr<File>
NfsFilesystem::root()
{
    if (!root_) {
        nfs_fh4 rootfh;
        fattr4 rootattr;
        compound(
            [](auto& enc)
            {
                bitmap4 wanted;
                setSupportedAttrs(wanted);
                set(wanted, FATTR4_MAXREAD);
                set(wanted, FATTR4_MAXWRITE);
                enc.putrootfh();
                enc.getattr(wanted);
                enc.getfh();
            },
            [&rootfh, &rootattr](auto& dec)
            {
                dec.putrootfh();
                rootattr = move(dec.getattr().obj_attributes);
                rootfh = move(dec.getfh().object);
            });

        root_ = find(move(rootfh), move(rootattr));
        
        fsinfo_.maxread = root_->attr().maxread_;
        fsinfo_.maxwrite = root_->attr().maxwrite_;

        // Set the buffer size for the largest read or write request we will
        // make, allowing extra space for protocol overhead
        chan_->setBufferSize(512 + max(fsinfo_.maxread, fsinfo_.maxwrite));
    }
    return root_;
}

const FilesystemId&
NfsFilesystem::fsid() const
{
    static FilesystemId nullfsid;
    return nullfsid;
}

shared_ptr<File>
NfsFilesystem::find(const FileHandle& fh)
{
    throw system_error(ESTALE, system_category());
}

void
NfsFilesystem::unmount()
{
    // Clear our delegation cache to return the delegations back to
    // the server
    delegations_.clear();

    // XXX we should keep track of NfsOpenFile instances and
    // force-close them here

    unique_lock<mutex> lock(mutex_);

    // Destroy the session and client
    compoundNoSequence(
        [this](auto& enc)
        {
            enc.destroy_session(sessionid_);
        },
        [](auto& dec)
        {
            dec.destroy_session();
        });
    compoundNoSequence(
        [this](auto& enc)
        {
            enc.destroy_clientid(clientid_);
        },
        [](auto& dec)
        {
            dec.destroy_clientid();
        });

    sockman_->stop();
    cbsvc_.unbind(svcreg_);
    chan_.reset();
    cbthread_.join();
}

shared_ptr<NfsFile>
NfsFilesystem::find(const nfs_fh4& fh)
{
    unique_lock<mutex> lock(mutex_);
    return cache_.find(
        fh,
        [&](auto file) {
        },
        [&](auto id) {
            return nullptr;
        });
}

shared_ptr<NfsFile>
NfsFilesystem::find(nfs_fh4&& fh, fattr4&& attr)
{
    unique_lock<mutex> lock(mutex_);
    return cache_.find(
        fh,
        [&](auto file) {
            file->update(move(attr));
        },
        [&](auto id) {
            return make_shared<NfsFile>(
                shared_from_this(), move(fh), move(attr));
        });
}

shared_ptr<NfsDelegation>
NfsFilesystem::addDelegation(
    shared_ptr<NfsFile> file, shared_ptr<NfsOpenFile> of,
    open_delegation4&& delegation)
{
    unique_lock<mutex> lock(mutex_);
    stateid4 stateid;
    switch (delegation.delegation_type) {
    case OPEN_DELEGATE_NONE:
    case OPEN_DELEGATE_NONE_EXT:
        return nullptr;

    case OPEN_DELEGATE_READ:
        stateid = delegation.read().stateid;
        break;

    case OPEN_DELEGATE_WRITE:
        stateid = delegation.write().stateid;
        break;
    }
    auto d = make_shared<NfsDelegation>(file, of, move(delegation));
    delegations_.add(stateid, d);
    return d;
}

void
NfsFilesystem::connect()
{
    sequenceid4 sequence;
    compoundNoSequence(
        [this](auto& enc)
        {
            enc.exchange_id(clientOwner_, 0, state_protect4_a(SP4_NONE), {});
        },
        [this, &sequence](auto& dec)
        {
            auto resok = dec.exchange_id();
            clientid_ = resok.eir_clientid;
            sequence = resok.eir_sequenceid;
        });

    LOG(INFO) << "clientid: " << hex << clientid_
              << ", sequence: " << sequence;

    auto slotCount = thread::hardware_concurrency();

    compoundNoSequence(
        [this, sequence, slotCount](auto& enc)
        {
            vector<callback_sec_parms4> sec_parms;
            sec_parms.emplace_back(AUTH_NONE);
            enc.create_session(
                clientid_, sequence,
                CREATE_SESSION4_FLAG_CONN_BACK_CHAN,
                channel_attrs4{0, 65536, 65536, 65536, 32, slotCount, {}},
                channel_attrs4{0, 65536, 65536, 65536, 32, slotCount, {}},
                NFS4_CALLBACK,
                sec_parms);
        },
        [this](auto& dec)
        {
            auto resok = dec.create_session();
            sessionid_ = resok.csr_sessionid;
            highestSlot_ = 0;
            targetHighestSlot_ = resok.csr_fore_chan_attrs.ca_maxrequests;
            slots_.clear();
            for (int i = 0; i < targetHighestSlot_; i++)
                slots_.emplace_back(Slot{1, false});
        });

    LOG(INFO) << "sessionid: " << sessionid_;

    cbsvc_.setSlots(slotCount);

    // XXX recover delegations and opens here
}

void
NfsFilesystem::handleCallbacks()
{
    auto sockchan = dynamic_pointer_cast<oncrpc::Socket>(chan_);
    if (sockchan) {
        chan_->onReconnect(
            [this, sockchan]() {
                sockman_->add(sockchan);
            });
        sockman_->add(sockchan);
        sockman_->run();
    }
}

pair<shared_ptr<Filesystem>, string>
NfsFilesystemFactory::mount(FilesystemManager* fsman, const string& url)
{
    using namespace oncrpc;

#if 0
    static map<int, string> flavors {
        { AUTH_NONE, "none" },
        { AUTH_SYS, "sys" },
        { RPCSEC_GSS_KRB5, "krb5" },
        { RPCSEC_GSS_KRB5I, "krb5i" },
        { RPCSEC_GSS_KRB5P, "krb5p" },
    };
#endif

    UrlParser p(url);

    auto chan = Channel::open(url, "tcp");
    auto client = make_shared<oncrpc::SysClient>(NFS4_PROGRAM, NFS_V4);
    auto clock = make_shared<detail::SystemClock>();

    return make_pair(
        fsman->mount<NfsFilesystem>(
            p.host + ":/", chan, client, clock),
        p.path);
}

void filesys::nfs4::init(FilesystemManager* fsman)
{
    fsman->add(make_shared<NfsFilesystemFactory>());
}
