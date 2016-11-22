/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <cassert>
#include <chrono>
#include <iomanip>
#include <random>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "paxos.h"

using namespace keyval;
using namespace keyval::paxos;

static std::random_device rnd;

UUID UUID::null = {
    0, 0, 0, 0,  0, 0, 0, 0,  0, 0, 0, 0,  0, 0, 0, 0
};

UUID::UUID()
    : UUID(null)
{
}

UUID::UUID(const UUID& other)
    : std::array<uint8_t, 16>(other)
{
}

UUID::UUID(std::initializer_list<uint8_t> init)
{
    assert(init.size() == 16);
    std::copy_n(init.begin(), 16, begin());
}

std::ostream& keyval::paxos::operator<<(std::ostream& os, const UUID& id)
{
    auto savefill = os.fill();
    auto saveflags = os.flags();
    os << std::hex << std::setfill('0');
    os << std::setw(2) << int(id[0]);
    os << std::setw(2) << int(id[1]);
    os << std::setw(2) << int(id[2]);
    os << std::setw(2) << int(id[3]);
    os << "-";
    os << std::setw(2) << int(id[4]);
    os << std::setw(2) << int(id[5]);
    os << "-";
    os << std::setw(2) << int(id[6]);
    os << std::setw(2) << int(id[7]);
    os << "-";
    os << std::setw(2) << int(id[8]);
    os << std::setw(2) << int(id[9]);
    os << "-";
    os << std::setw(2) << int(id[10]);
    os << std::setw(2) << int(id[11]);
    os << std::setw(2) << int(id[12]);
    os << std::setw(2) << int(id[13]);
    os << std::setw(2) << int(id[14]);
    os << std::setw(2) << int(id[15]);
    os.fill(savefill);
    os.flags(saveflags);
    return os;
}

static UUID makeUUID()
{
    UUID id;
    for (int i = 0; i < 16; i++)
        id[i] = rnd();
    id[6] = (id[6] & 0x0f) | 0x40; // random uuid
    id[8] = (id[8] & 0x3f) | 0x80;
    return id;
}

std::ostream& keyval::paxos::operator<<(std::ostream& os, const PaxosRound& i)
{
    os << "PaxosRound(" << i.gen << "," << i.id << ")";
    return os;
}

Replica::Replica(
    std::shared_ptr<IPaxos1> proto,
    std::shared_ptr<util::Clock> clock,
    std::shared_ptr<oncrpc::TimeoutManager> tman,
    std::shared_ptr<Database> db)
    : proto_(proto),
      svcreg_(std::make_shared<oncrpc::ServiceRegistry>()),
      clock_(clock),
      tman_(tman),
      db_(db),
      status_(STATUS_HEALTHY),
      leader_(UUID::null),
      isLeader_(false),
      newLeader_(false)
{
    // We use the PaxosLog namespace to record the state of the
    // distributed consensus. Keys are XDR-encoded instance numbers
    // and values are XDR-encoded AcceptorState structures.
    //
    // Derived classes are responsible for tracking the last-committed
    // instance in the PaxosMeta namespace in an entry with key
    // "instance" and value the XDR-encoded instance number which was
    // committed.
    meta_ = db_->getNamespace("PaxosMeta");
    log_ = db_->getNamespace("PaxosLog");

    // Read our UUID from the db if present.
    try {
        auto val = meta_->get(std::make_shared<Buffer>("uuid"));
        oncrpc::XdrMemory xmv(val->data(), val->size());
        xdr(uuid_, static_cast<oncrpc::XdrSource*>(&xmv));
    }
    catch (std::system_error& e) {
        uuid_ = makeUUID();
        auto val = std::make_shared<Buffer>(oncrpc::XdrSizeof(uuid_));
        oncrpc::XdrMemory xmv(val->data(), val->size());
        xdr(uuid_, static_cast<oncrpc::XdrSink*>(&xmv));
        auto trans = db_->beginTransaction();
        trans->put(meta_, std::make_shared<Buffer>("uuid"), val);
        db_->commit(std::move(trans));
    }

    // Read the last-committed instance number from the db
    try {
        auto val = meta_->get(std::make_shared<Buffer>("instance"));
        oncrpc::XdrMemory xmv(val->data(), val->size());
        xdr(appliedInstance_, static_cast<oncrpc::XdrSource*>(&xmv));
        maxInstance_ = appliedInstance_;
    }
    catch (std::system_error& e) {
    }

    VLOG(1) << this << ": last applied instance: " << appliedInstance_;

    identity({uuid_, status_, maxInstance_});
    std::unique_lock<std::mutex> lk(mutex_);
    updateLeaderTimer(lk);
    sendIdentity(lk);
}

static std::shared_ptr<oncrpc::Channel> connectChannel(
    const std::vector<std::string>& replicas)
{
    using namespace oncrpc;
    std::vector<AddressInfo> addrs;
    for (auto& replica: replicas) {
        auto t = getAddressInfo(replica);
        addrs.insert(addrs.end(), t.begin(), t.end());
    }
    for (auto& ai: addrs)
        LOG(INFO) << "connect: " << ai.host() << ":" << ai.port();
    return Channel::open(addrs, true);
}

Replica::Replica(
    const std::string& addr,
    const std::vector<std::string>& replicas,
    std::shared_ptr<util::Clock> clock,
    std::shared_ptr<oncrpc::SocketManager> sockman,
    std::shared_ptr<Database> db)
    : Replica(
        std::make_shared<Paxos1<oncrpc::SysClient>>(connectChannel(replicas)),
        clock, sockman, db)
{
    bind(svcreg_);

    // Bind to the given address so that we can receive messages from
    // the other replicas
    //
    // For replicated metadata servers, the given addr may have
    // several addresses, some of which are not for this server. Just
    // ignore any bind errors but make sure we bind to at least one
    // address.
    int bound = false;
    std::exception_ptr lastError;
    for (auto& ai: oncrpc::getAddressInfo(addr)) {
        try {
            int fd = socket(ai.family, ai.socktype, ai.protocol);
            if (fd < 0)
                throw std::system_error(errno, std::system_category());
            int one = 1;
            ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
            ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
            auto sock = std::make_shared<oncrpc::DatagramChannel>(fd, svcreg_);
            sock->bind(ai.addr);
            sock->connect(ai.addr);
            sockman->add(sock);
            LOG(INFO) << "bind: " << ai.host() << ":" << ai.port();
            bound = true;
        }
        catch (std::system_error& e) {
            lastError = std::current_exception();
        }
    }
    if (!bound) {
        std::rethrow_exception(lastError);
    }
}

Replica::~Replica()
{
    tman_->cancel(identityTimer_);
    for (auto& entry: proposerState_) {
        auto pp = entry.second.get();
        if (pp->prepareTimer)
            tman_->cancel(pp->prepareTimer);
        if (pp->acceptTimer)
            tman_->cancel(pp->acceptTimer);
    }
}

void Replica::null()
{
}

void Replica::identity(const IDENTITYargs& args)
{
    std::unique_lock<std::mutex> lk(mutex_);
    auto instance = args.instance;

    if (VLOG_IS_ON(3)) {
        VLOG(3) << this << ": " << instance << ": received identity from "
                << args.uuid;
    }

    auto newPeer = peers_.find(args.uuid) == peers_.end();
    auto& p = peers_[args.uuid];
    p.when = clock_->now();
    p.status = args.status;
    p.appdata = args.appdata;
    updatePeers(lk);
    if (newPeer)
        LOG(INFO) << this << ": " << peers_.size()
                  << (peers_.size() > 1 ? " peers" : " peer");
}

void Replica::prepare(const PREPAREargs& args)
{
    std::unique_lock<std::mutex> lk(mutex_);
    auto instance = args.instance;
    auto& i = args.i;

    if (VLOG_IS_ON(2))
        VLOG(2) << this << ": " << instance << ": received prepare " << i;
    auto ap = findAcceptorState(lk, instance, true);
    if (i > ap->rnd) {
        if (instance > maxInstance_)
            setLeader(lk, args.uuid);
        if (VLOG_IS_ON(2))
            VLOG(2) << this << ": " << instance << ": sending promise " << i;
        ap->rnd = i;
        saveAcceptorState(lk, ap);
        proto_->promise({uuid_, instance, i, ap->vrnd, ap->vval});
    } else if (i != ap->rnd) {
        VLOG(2) << this << ": " << instance << ": sending nack " << ap->rnd;
        proto_->nack({uuid_, instance, ap->rnd});
    }
}

void Replica::promise(const PROMISEargs& args)
{
    std::unique_lock<std::mutex> lk(mutex_);
    auto instance = args.instance;
    auto& i = args.i;
    auto& vrnd = args.vrnd;
    auto& vval = args.vval;

    // Only process the message if we have an active proposal
    auto pp = findProposerState(lk, instance, false);
    if (!pp)
        return;

    if (pp->state == ProposerState::PHASE1 && i == pp->crnd) {
        pp->promisers.insert(args.uuid);

        if (VLOG_IS_ON(2))
            VLOG(2) << this << ": " << instance << ": received promise " << i
                    << " reply count: " << pp->promisers.size();

        if (vrnd > pp->largestVrnd) {
            VLOG(2) << this << ": " << instance
                    << ": received promise vrnd " << vrnd
                    << ": vval {" << vval.size() << " bytes}";
            assert(vrnd > PaxosRound{0});
            pp->largestVrnd = vrnd;
            pp->cval = vval;
        }

        if (pp->promisers.size() >= quorum()) {
            // Make sure we have at least one acceptor that isn't us
            assert(quorum() > 1);

            // We have replies from a majority of acceptors so we can
            // move forward to phase 2
            tman_->cancel(pp->prepareTimer);
            pp->prepareTimer = 0;

            // If a value was voted on in some previous round, we must use
            // that value otherwise we take the next command from our pending
            // list as the value. If we have no pending commands, just execute
            // a null command
            if (pp->largestVrnd == PaxosRound{0}) {
                // If we are recovering, there should have been a
                // value suggested by one of our peers. If none of
                // them have a value, this must be an empty
                // transaction.
                if (status_ != STATUS_RECOVERING &&
                    pendingCommands_.size() > 0) {
                    pp->transaction = pendingCommands_.front();
                    pendingCommands_.pop_front();
                    pp->cval = pp->transaction->value();
                }
                else {
                    pp->cval = {};
                }
                pp->largestVrnd = pp->crnd;
            }
            sendAccept(lk, pp);
        }
    }
}

void Replica::accept(const ACCEPTargs& args)
{
    std::unique_lock<std::mutex> lk(mutex_);
    auto instance = args.instance;
    auto& i = args.i;
    auto& v = args.v;

    auto ap = findAcceptorState(lk, instance, true);
    if (i >= ap->rnd && i != ap->vrnd) {
        if (VLOG_IS_ON(2))
            VLOG(2) << this << ": " << instance << ": received accept " << i;

        updateLeaderTimer(lk);
        if (instance > maxInstance_) {
            setLeader(lk, args.uuid);
            maxInstance_ = instance;
        }

        ap->rnd = i;
        ap->vrnd = i;
        ap->vval = v;
        if (VLOG_IS_ON(2))
            VLOG(2) << this << ": " << instance << ": sending accepted " << i
                    << " {" << v.size() << " bytes}";
        saveAcceptorState(lk, ap);
        proto_->accepted({uuid_, instance, i, ap->vval});
    } else {
        proto_->nack({uuid_, instance, ap->rnd});
    }
}

void Replica::accepted(const ACCEPTargs& args)
{
    std::unique_lock<std::mutex> lk(mutex_);
    auto instance = args.instance;
    auto& i = args.i;
    auto& v = args.v;

    // Don't bother learning values we have already applied to our
    // state machine
    if (instance <= appliedInstance_)
        return;

    if (instance > maxInstance_)
        maxInstance_ = instance;

    auto lp = findLearnerState(lk, instance, true);
    auto it = lp->acceptors.find(args.uuid);
    if (it == lp->acceptors.end()) {
        lp->values[v]++;
        lp->acceptors.insert(args.uuid);
    }
    if (VLOG_IS_ON(2))
        VLOG(2) << this << ": " << instance << ": received accepted " << i
                << " reply count: " << lp->acceptors.size()
                << " {" << v.size() << " bytes}";

    // See if there is a consensus on a value
    int maxCount = 0;
    const PaxosCommand* value = nullptr;
    for (auto& entry: lp->values) {
        if (entry.second > maxCount) {
            value = &entry.first;
            maxCount = entry.second;
        }
    }

    if (maxCount == quorum()) {;
        // Make sure we have at least one acceptor that isn't us
        assert(quorum() > 1);

        // We have received accepted replies from a majority of
        // acceptors so the value can be learned
        VLOG(2) << this << ": " << instance << ": completed " << lp;
        lp->value = value;

        // If this is the most recent transaction, update the leader
        if (instance == maxInstance_)
            setLeader(lk, i.id);

        // If we also proposed the value, stop our re-send timer
        // and inform the client
        auto pp = findProposerState(lk, instance, false);
        if (pp) {
            if (isLeader_) {
                // Clear the newLeader flag if there was no conflict with
                // some other proposer
                if (pp->nackCount == 0) {
                    newLeader_ = false;
                }

                // Reset the lease timer since we have successfully
                // executed a transaction
                updateLeaseTimer(lk);
            }
            pp->state = ProposerState::COMPLETE;
            if (pp->acceptTimer) {
                tman_->cancel(pp->acceptTimer);
                pp->acceptTimer = 0;
            }
            if (pp->prepareTimer) {
                tman_->cancel(pp->prepareTimer);
                pp->prepareTimer = 0;
            }
            if (pp->transaction) {
                pp->transaction->complete();
                pp->transaction.reset();
            }
            proposerState_.erase(instance);
            activeInstances_--;
        }

        if (applyCommands(lk)) {
            // If we are fully up to date and still have commands
            // pending start a new round to try and get them executed
            if (isLeader_ && pendingCommands_.size() > 0 &&
                activeInstances_ == 0) {
                startNewInstance(lk, maxInstance_ + 1);
            }

            if (status_ == STATUS_RECOVERING) {
                // We have values for all known instances so we are
                // healthy again
                LOG(INFO) << this << ": recovered to " << maxInstance_;
                status_ = STATUS_HEALTHY;
                sendIdentity(lk);
            }
        }
        progress_.notify_all();
    }
}

void Replica::nack(const NACKargs& args)
{
    std::unique_lock<std::mutex> lk(mutex_);
    auto instance = args.instance;
    auto& i = args.i;

    auto pp = findProposerState(lk, instance, false);
    if (pp) {
        VLOG(2) << this << ": " << instance << ": received nack " << i;
        VLOG(2) << this << ": " << instance << ": current crnd " << pp->crnd;
        if (pp->crnd.gen > 0 && i > pp->crnd) {
            pp->nackCount++;
            pp->crnd = PaxosRound{i.gen + 1, uuid_};
            sendPrepare(lk, pp);
        }
    }
}

std::shared_ptr<PendingTransaction> Replica::execute(
        const std::vector<uint8_t>& command)
{
    std::unique_lock<std::mutex> lk(mutex_);
    auto trans = std::make_shared<PendingTransaction>(command);
    pendingCommands_.push_back(trans);
    auto pp = startNewInstance(lk, maxInstance_ + 1);
    VLOG(2) << this << ": " << pp->instance
            << ": executing command in new instance";
    return trans;
}

void Replica::forceLeader()
{
    std::unique_lock<std::mutex> lk(mutex_);
    leader_ = uuid_;
    isLeader_ = true;
    newLeader_ = true;
    updateLeaseTimer(lk);
}

ProposerState* Replica::findProposerState(
    std::unique_lock<std::mutex>& lk, std::int64_t instance, bool create)
{
    auto i = proposerState_.find(instance);
    if (i == proposerState_.end()) {
        if (!create)
            return nullptr;
        std::tie(i, std::ignore) = proposerState_.insert(
            std::make_pair(
                instance, std::make_unique<ProposerState>(instance)));
        VLOG(2) << this << ": " << instance
                << ": created new proposer " << i->second.get();
    }
    return i->second.get();
}

LearnerState* Replica::findLearnerState(
    std::unique_lock<std::mutex>& lk, std::int64_t instance, bool create)
{
    auto i = learnerState_.find(instance);
    if (i == learnerState_.end()) {
        if (!create)
            return nullptr;
        auto lp = std::make_unique<LearnerState>(instance);
        lp->time = clock_->now();
        std::tie(i, std::ignore) = learnerState_.insert(
            std::make_pair(instance, std::move(lp)));
        VLOG(2) << this << ": " << instance
                << ": created new learner " << i->second.get();
    }
    return i->second.get();
}

std::shared_ptr<AcceptorState> Replica::findAcceptorState(
    std::unique_lock<std::mutex>& lk, std::int64_t instance, bool create)
{
    return acceptorState_.find(
        instance,
        [](auto) {},
        [this, create](std::int64_t instance) ->
        std::shared_ptr<AcceptorState> {
            try {
                auto key = std::make_shared<Buffer>(
                    oncrpc::XdrSizeof(instance));
                oncrpc::XdrMemory xmk(key->data(), key->size());
                xdr(instance, static_cast<oncrpc::XdrSink*>(&xmk));

                auto val = log_->get(key);
                oncrpc::XdrMemory xmv(val->data(), val->size());

                auto ap = std::make_shared<AcceptorState>(instance);
                VLOG(2) << this << ": " << instance
                        << ": restored acceptor from db" << ap.get();
                xdr(*ap, static_cast<oncrpc::XdrSource*>(&xmv));

                return ap;
            }
            catch (std::system_error& e) {
                if (e.code().value() != ENOENT)
                    throw;
                if (!create)
                    return nullptr;
                auto ap = std::make_shared<AcceptorState>(instance);
                VLOG(2) << this << ": " << instance
                        << ": created new acceptor " << ap.get();
                return ap;
            }
        });
}

ProposerState* Replica::startNewInstance(
    std::unique_lock<std::mutex>& lk, std::int64_t instance)
{
    activeInstances_++;
    auto pp = findProposerState(lk, instance, true);
    if (pp->state == ProposerState::INIT) {
        assert(pp->crnd == PaxosRound{0});
        if (!isLeader_ || newLeader_) {
            // If we are a follower trying to catch up or if we have
            // only just become leader, run the full protocol
            pp->crnd = PaxosRound{1, uuid_};
            sendPrepare(lk, pp);
            lk.lock();
        }
        else {
            // Otherwise just send accept right away with the value of
            // our choice
            pp->crnd = crnd_;
            if (pendingCommands_.size() > 0) {
                pp->transaction = pendingCommands_.front();
                pendingCommands_.pop_front();
                pp->cval = pp->transaction->value();
            }
            else {
                pp->cval = {};
            }
            pp->largestVrnd = pp->crnd;
            sendAccept(lk, pp);
            lk.lock();
        }
    }
    return pp;
}

void Replica::sendIdentity(std::unique_lock<std::mutex>& lk)
{
    if (VLOG_IS_ON(3)) {
        VLOG(3) << this << ": " << maxInstance_
                << ": " << uuid_ << ": sending identity";
    }
    if (identityTimer_)
        tman_->cancel(identityTimer_);

    // Set the timer to somewhere between LWT/2 and 3*LWT/4
    auto fuzz = std::uniform_int_distribution<>(0, 99)(rnd);
    identityTimer_ = tman_->add(
        clock_->now() +
        (LEADER_WAIT_TIME / 2) +
        (LEADER_WAIT_TIME * fuzz / 400),
        [this]() {
            std::unique_lock<std::mutex> lk2(mutex_);
            identityTimer_ = 0;
            sendIdentity(lk2);
        });

    lk.unlock();
    proto_->identity(IDENTITYargs{uuid_, status_, maxInstance_, appdata_});
}

void Replica::sendPrepare(std::unique_lock<std::mutex>& lk, ProposerState* pp)
{
    VLOG(2) << this << ": " << pp->instance
            << ": sending prepare " << pp->crnd;
    if (pp->prepareTimer)
        tman_->cancel(pp->prepareTimer);
    pp->prepareTimer =
        tman_->add(
            clock_->now() + rtt_,
            [this, pp]() {
                LOG(INFO) << this << ": " << pp->instance
                          << ": prepare timeout";
                std::unique_lock<std::mutex> lk2(mutex_);
                if (pp->state == ProposerState::PHASE1) {
                    pp->prepareTimer = 0;
                    pp->crnd.gen++;
                    sendPrepare(lk2, pp);
                }
            });
    pp->state = ProposerState::PHASE1;
    pp->promisers.clear();
    crnd_ = pp->crnd;
    lk.unlock();
    proto_->prepare({uuid_, pp->instance, pp->crnd});
}

void Replica::sendAccept(std::unique_lock<std::mutex>& lk, ProposerState* pp)
{
    VLOG(2) << this << ": " << pp->instance
            << ": sending accept " << pp->crnd
            << " {" << pp->cval.size() << " bytes}";
    if (pp->acceptTimer)
        tman_->cancel(pp->acceptTimer);
    pp->acceptTimer =
        tman_->add(
            clock_->now() + rtt_,
            [this, pp]() {
                LOG(INFO) << this << ": " << pp->instance
                          << ": accept timeout";
                std::unique_lock<std::mutex> lk2(mutex_);
                if (pp->state == ProposerState::PHASE2) {
                    pp->acceptTimer = 0;
                    pp->crnd.gen++;
                    sendAccept(lk2, pp);
                }
            });
    pp->state = ProposerState::PHASE2;
    lk.unlock();
    proto_->accept({uuid_, pp->instance, pp->crnd, pp->cval});
}

void Replica::updatePeers(std::unique_lock<std::mutex>& lk)
{
    auto cutoff = clock_->now() - LEADER_WAIT_TIME;

    healthyCount_ = 0;
    recoveringCount_ = 0;
    for (const auto& p : peers_) {
        if (p.second.when >= cutoff) {
            if (p.second.status == STATUS_HEALTHY)
                healthyCount_++;
            if (p.second.status == STATUS_RECOVERING)
                recoveringCount_++;
        }
    }
}

void Replica::setLeader(std::unique_lock<std::mutex>& lk, const UUID& id)
{
    // Make sure we don't think we are leader if we are recovering
    if (status_ == STATUS_RECOVERING)
        return;

    if (id != leader_) {
        auto wasLeader = isLeader_;
        leader_ = id;
        isLeader_ = (leader_ == uuid_);
        if (isLeader_ != wasLeader) {
            if (isLeader_) {
                LOG(INFO) << this << ": becoming leader";
                newLeader_ = true;
            }
            else {
                if (leaseTimer_) {
                    tman_->cancel(leaseTimer_);
                    leaseTimer_ = 0;
                }
                LOG(INFO) << this << ": becoming follower";
            }
            leaderChanged();
        }
    }
}

void Replica::updateLeaderTimer(std::unique_lock<std::mutex>& lk)
{
    if (leaderTimer_)
        tman_->cancel(leaderTimer_);
    if (!leaderElections_)
        return;
    leaderTimer_ =
        tman_->add(
            clock_->now() + LEADER_WAIT_TIME,
            [this]() {
                LOG(INFO) << this << ": leadership timeout";
                std::unique_lock<std::mutex> lk2(mutex_);
                startNewInstance(lk2, maxInstance_ + 1);
            });
}

void Replica::updateLeaseTimer(std::unique_lock<std::mutex>& lk)
{
    if (leaseTimer_)
        tman_->cancel(leaseTimer_);
    if (!leaderElections_)
        return;
    leaseTimer_ =
        tman_->add(
            clock_->now() + 3*LEADER_WAIT_TIME/4,
            [this]() {
                VLOG(2) << this << ": extending lease";
                std::unique_lock<std::mutex> lk2(mutex_);
                startNewInstance(lk2, maxInstance_ + 1);
            });
}

bool Replica::applyCommands(std::unique_lock<std::mutex>& lk)
{
    auto now = clock_->now();
    while (appliedInstance_ < maxInstance_) {
        auto instance = appliedInstance_ + 1;
        auto lp = findLearnerState(lk, instance, false);
        if (!lp || (now - lp->time) > 10*LEADER_WAIT_TIME) {
            // If we don't have a learner state entry for the next
            // instance to apply or if the state we do have is too
            // old, attempt to recover the gap. Note: since we are
            // called after we see a quorum of accepted replies for
            // maxInstance, we know that we have missed an instance.
            //
            // If the gap is larger than one instance, we will end up
            // here again after we recover a value for this instance.

            VLOG(2) << this <<": recovering instance " << instance;
            if (status_ != STATUS_RECOVERING)
                LOG(INFO) << this << ": recovering from " << instance;
            status_ = STATUS_RECOVERING;
            if (isLeader_) {
                // Some other replica is creating new instances so
                // clearly they believe themselves to be leader. At
                // this point, its not clear who is leader but its
                // certainly not us.
                setLeader(lk, UUID::null);
            }
            startNewInstance(lk, instance);
            sendIdentity(lk);
            return false;
        }
        if (!lp->applied && lp->value) {
            if (lp->value->size() > 0) {
                apply(instance, *lp->value);
            }
            else {
                VLOG(2) << this << ": " << instance
                        << ": empty command - not applying";

                // We do need to write the instance number
                auto trans = db()->beginTransaction();
                saveInstance(instance, trans.get());
                db()->commit(std::move(trans));
            }
            lp->applied = true;
            //learnerState_.erase(instance);
            appliedInstance_++;
        }
        else {
            return false;
        }
    }
    return true;
}

void Replica::saveAcceptorState(
    std::unique_lock<std::mutex>& lk, std::shared_ptr<AcceptorState> ap)
{
    auto key = std::make_shared<Buffer>(oncrpc::XdrSizeof(ap->instance));
    oncrpc::XdrMemory xmk(key->data(), key->size());
    xdr(ap->instance, static_cast<oncrpc::XdrSink*>(&xmk));

    auto val = std::make_shared<Buffer>(oncrpc::XdrSizeof(*ap));
    oncrpc::XdrMemory xmv(val->data(), val->size());
    xdr(*ap, static_cast<oncrpc::XdrSink*>(&xmv));

    auto trans = db_->beginTransaction();
    trans->put(log_, key, val);
}

void Replica::saveInstance(std::int64_t instance, Transaction* trans)
{
    auto val = std::make_shared<Buffer>(oncrpc::XdrSizeof(instance));
    oncrpc::XdrMemory xmv(val->data(), val->size());
    xdr(instance, static_cast<oncrpc::XdrSink*>(&xmv));
    trans->put(meta_, std::make_shared<Buffer>("instance"), val);
}