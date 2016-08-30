/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <array>
#include <condition_variable>
#include <mutex>
#include <unordered_map>

#include <keyval/keyval.h>
#include <rpc++/timeout.h>
#include <util/lrucache.h>
#include <util/util.h>

namespace keyval {
namespace paxos {

struct UUID: public std::array<uint8_t, 16>
{
    UUID();
    UUID(const UUID& other);
    UUID(std::initializer_list<uint8_t> init);

    static UUID null;
};

std::ostream& operator<<(std::ostream& os, const UUID& id);

}
}

#include "keyval/paxos/paxosproto.h"

namespace std
{
template<> struct hash<keyval::paxos::UUID>
{
    typedef keyval::paxos::UUID argument_type;
    typedef std::size_t result_type;

    template<typename ARRAY>
    static int _djb2(const ARRAY& a, int seed = 5381)
    {
        size_t hash = seed;
        for (auto c: a)
            hash = (hash << 5) + hash + c; /* hash * 33 + c */
        return hash;
    }

    result_type operator()(const argument_type& id) const
    {
        return _djb2(id);
    }
};
}

namespace keyval {
namespace paxos {

std::ostream& operator<<(std::ostream& os, const PaxosRound& i);

static inline int operator==(const PaxosRound& i, const PaxosRound& j)
{
    return i.gen == j.gen && i.id == j.id;
}

static inline int operator!=(const PaxosRound& i, const PaxosRound& j)
{
    return i.gen != j.gen || i.id != j.id;
}

static inline int operator>(const PaxosRound& i, const PaxosRound& j)
{
    return i.gen > j.gen || (i.gen == j.gen && i.id > j.id);
}

static inline int operator<=(const PaxosRound& i, const PaxosRound& j)
{
    return i.gen < j.gen || (i.gen == j.gen && i.id <= j.id);
}

static inline int operator>=(const PaxosRound& i, const PaxosRound& j)
{
    return i.gen > j.gen || (i.gen == j.gen && i.id >= j.id);
}

static constexpr util::Clock::duration LEADER_WAIT_TIME =
    std::chrono::seconds(2);

/// Track the status of a transaction which is being executed on a set
/// of replicas
class PendingTransaction
{
public:
    PendingTransaction(const std::vector<uint8_t>& value)
        : value_(value),
          completed_(false)
    {
    }

    auto& value() const { return value_; }

    /// Wait until the transaction is completed
    void wait()
    {
        std::unique_lock<std::mutex> lk(mutex_);
        if (!completed_)
            cv_.wait(lk);
    }

    /// Set the completed flag and wake up any sleepers
    void complete()
    {
        std::unique_lock<std::mutex> lk(mutex_);
        completed_ = true;
        cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<uint8_t> value_;
    bool completed_;
};

/// The proposer state for a single Paxos instance
///
struct ProposerState
{
    ProposerState() : instance(0)
    {
    }

    ProposerState(std::int64_t instance) : instance(instance)
    {
    }

    enum {
        /// Initial state, prepare not yet sent
        INIT,

        /// Prepare has been sent, wating for a quorum of promise
        /// replies
        PHASE1,

        /// Accept is sent, waiting for a quorum of accepted replies
        PHASE2,

        /// Instance has been accepted by a quorum and is therefore
        /// stable
        COMPLETE
    } state = INIT;

    /// Instance number
    ///
    std::int64_t instance;

    /// The last round we have tried to propose
    ///
    PaxosRound crnd = {0};

    /// The command we last tried to get accepted
    ///
    PaxosCommand cval = {};

    /// The greatest value of vrnd reported by any promise message
    /// so far
    ///
    PaxosRound largestVrnd = {0};

    /// The set of nodes which we have received promises from
    ///
    std::unordered_set<UUID> promisers;

    /// Number of nack replies received so far
    ///
    int nackCount = 0;

    /// Re-send prepare if we time out without receiving enough
    /// promise replies from the other replicas
    ///
    oncrpc::TimeoutManager::task_type prepareTimer = 0;

    /// Re-send accept if we time out without receiving enough
    /// accepted replies from the other replicas
    ///
    oncrpc::TimeoutManager::task_type acceptTimer = 0;

    /// If this instance executed one of our own transactions, this
    /// points at the pending transaction object
    std::shared_ptr<PendingTransaction> transaction;
};

/// The acceptor state for a single Paxos instance. This information
/// should be stored persistently in stable storage.
///
struct AcceptorState
{
    AcceptorState() : instance(0)
    {
    }

    AcceptorState(std::int64_t instance) : instance(instance)
    {
    }

    /// Instance number
    ///
    std::int64_t instance;

    /// The highest round we have received a prepare message for
    ///
    PaxosRound rnd = {0};

    /// The highest round we voted in
    ///
    PaxosRound vrnd = {0};

    /// The value we last voted for
    ///
    PaxosCommand vval = {};
};

template <typename XDR>
void xdr(oncrpc::RefType<AcceptorState, XDR> v, XDR* xdrs)
{
    xdr(v.rnd, xdrs);
    xdr(v.vrnd, xdrs);
    xdr(v.vval, xdrs);
}

/// The learner state for a single Paxos instance
///
struct LearnerState
{
    LearnerState() : instance(0)
    {
    }

    LearnerState(std::int64_t instance) : instance(instance)
    {
    }

    /// Instance number
    ///
    std::int64_t instance;

    /// The time we started tracking learning state for this
    /// instance. We use this to detect stale entries, e.g. to recover
    /// from missed accepted messages.
    util::Clock::time_point time;

    /// The set of accepted replies seen so far along with the count
    /// of how many acceptors agree on each value
    ///
    std::map<PaxosCommand, int> values;

    /// To make sure we don't double count if an acceptor re-sends its
    /// accepted message, track which acceptors have voted
    ///
    std::unordered_set<UUID> acceptors;

    /// The consensus value, i.e. the value selected by a majority of
    /// acceptors. Note that the quorum size ensures there can be only
    /// one consensus value since it is strictly greater than half the
    /// number of acceptors.
    ///
    const PaxosCommand* value = nullptr;

    /// True if the value has been applied to the state machine
    ///
    bool applied = false;
};

/// An implementation of a replicated log using the Paxos algorithm
class Replica: public Paxos1Service
{
public:

    Replica(
        std::shared_ptr<IPaxos1> proto,
        std::shared_ptr<util::Clock> clock,
        std::shared_ptr<oncrpc::TimeoutManager> tman,
        std::shared_ptr<Database> db);

    Replica(
        const std::string& replicaAddress,
        std::shared_ptr<util::Clock> clock,
        std::shared_ptr<oncrpc::SocketManager> sockman,
        std::shared_ptr<Database> db);

    virtual ~Replica();

    // IPaxos1 overrides
    void null() override;
    void identity(const IDENTITYargs& args) override;
    void prepare(const PREPAREargs& args) override;
    void promise(const PROMISEargs& args) override;
    void accept(const ACCEPTargs& args) override;
    void accepted(const ACCEPTargs& args) override;
    void nack(const NACKargs& args) override;

    /// Execute a state machine command using the Paxos protocol
    std::shared_ptr<PendingTransaction> execute(
        const std::vector<uint8_t>& command);

    /// Once a command has been accepted by a round of the Paxos
    /// protocol, apply it to the state machine to move forward to the
    /// next state.
    virtual void apply(
        std::int64_t instance, const std::vector<uint8_t>& command) = 0;

    /// Called when the value of isLeader_ changes
    virtual void leaderChanged() = 0;

    auto& uuid() const { return uuid_; }
    auto db() const { return db_.get(); }
    auto peers() const { return int(peers_.size()); }
    auto healthy() const { return healthyCount_; }
    auto recovering() const { return recoveringCount_; }
    auto quorum() const
    {
        int q = peers() / 2 + 1;
        if (q >= minimumQuorum_)
            return q;
        else
            return minimumQuorum_;
    }
    auto isLeader() const { return isLeader_; }
    auto activeInstances() const { return activeInstances_; }
    auto disableLeaderElections() { leaderElections_ = false; }

    /// Force this replica to believe it is leader - used in unit
    /// tests
    void forceLeader();

protected:
    struct PeerState {
        util::Clock::time_point when;
        ReplicaStatus status;
    };

    /// Find or create a state structure for the given instance
    ProposerState* findProposerState(
        std::unique_lock<std::mutex>& lk, std::int64_t instance, bool create);
    LearnerState* findLearnerState(
        std::unique_lock<std::mutex>& lk, std::int64_t instance, bool create);
    std::shared_ptr<AcceptorState> findAcceptorState(
        std::unique_lock<std::mutex>& lk, std::int64_t instance, bool create);

    /// Start a new paxos instance and send a prepare message
    ProposerState* startNewInstance(
        std::unique_lock<std::mutex>& lk, std::int64_t instance);

    /// Send an identity message and queue a timeout for the next
    /// one. The lock will be unlocked on exit
    void sendIdentity(std::unique_lock<std::mutex>& lk);

    /// Send a prepare message and queue a retry in case of message
    /// loss. The lock will be unlocked on exit
    void sendPrepare(std::unique_lock<std::mutex>& lk, ProposerState* pp);

    /// Send a prepare message and queue a retry in case of message
    /// loss. The lock will be unlocked on exit
    void sendAccept(std::unique_lock<std::mutex>& lk, ProposerState* pp);

    void updatePeers(std::unique_lock<std::mutex>& lk);
    void setLeader(std::unique_lock<std::mutex>& lk, const UUID& id);

    /// Called from accept to update the leader failure timer
    void updateLeaderTimer(std::unique_lock<std::mutex>& lk);

    /// Called from accepted to update the leadership lease timer
    void updateLeaseTimer(std::unique_lock<std::mutex>& lk);

    /// Apply any commands we have learned, ensuring that commands are
    /// applied stricly in sequence. Returns true if we have
    /// successfully applied all commands up to maxInstance_
    bool applyCommands(std::unique_lock<std::mutex>& lk);

    /// Write an acceptor state entry to the db
    void saveAcceptorState(
        std::unique_lock<std::mutex>& lk, std::shared_ptr<AcceptorState> ap);

    /// Add the current instance number to a transaction
    void saveInstance(std::int64_t instance, Transaction* trans);

    UUID uuid_;
    std::mutex mutex_;
    std::shared_ptr<IPaxos1> proto_;
    std::shared_ptr<oncrpc::ServiceRegistry> svcreg_;
    std::shared_ptr<util::Clock> clock_;
    std::shared_ptr<oncrpc::TimeoutManager> tman_;
    std::shared_ptr<Database> db_;
    std::shared_ptr<Namespace> meta_;
    std::shared_ptr<Namespace> log_;
    ReplicaStatus status_;
    std::unordered_map<UUID, PeerState> peers_;
    int minimumQuorum_ = 2;
    int healthyCount_ = 0;
    int recoveringCount_ = 0;

    /// Approximate round-trip-time of the network. Used to determine
    /// re-send timers. Defaults to LEADER_WAIT_TIME.
    util::Clock::duration rtt_ = LEADER_WAIT_TIME;

    /// A timer which fires when its time to send an identify message
    oncrpc::TimeoutManager::task_type identityTimer_ = 0;

    /// We detect leader failure by setting a LEADER_WAIT_TIME timer
    /// each time a new transaction is created. If the timer fires,
    /// the replicas elect a new leader by attempting to execute an
    /// empty transaction.
    oncrpc::TimeoutManager::task_type leaderTimer_ = 0;

    /// If we are leader, we try to hold onto leadership by making
    /// sure we execute a transaction regularly, extending our
    /// leadership lease by LEADER_WAIT_TIME. This timer is used to
    /// ensure that if we have executed no other transactions for 3/4
    /// LEADER_WAIT_TIME.
    oncrpc::TimeoutManager::task_type leaseTimer_ = 0;

    /// Identity of current leader
    UUID leader_;

    /// True if we are the leader
    bool isLeader_;

    /// True if automatic leadership elections are enabled
    bool leaderElections_ = true;

    /// True if we have only just become leader - we need to run the
    /// full paxos protocol in the next instance
    bool newLeader_;

    /// The last round number we used as leader
    PaxosRound crnd_;

    /// The paxos state for all instances that we have participated in
    ///
    std::unordered_map<std::int64_t,
                       std::unique_ptr<ProposerState>> proposerState_;
    std::unordered_map<std::int64_t,
                       std::unique_ptr<LearnerState>> learnerState_;
    util::LRUCache<std::int64_t, AcceptorState> acceptorState_;

    /// The maximum paxos instance we have completed (i.e. learned a
    /// value)
    std::int64_t maxInstance_ = 0;

    /// The last instance which we have applied to our state machine
    std::int64_t appliedInstance_ = 0;

    /// The number of incomplete paxos instances which are currently
    /// in-flight
    int activeInstances_ = 0;

    /// Pending commands that have not yet been processed
    ///
    std::deque<std::shared_ptr<PendingTransaction>> pendingCommands_;
};

/// A specialisation of Replica which uses the replicated log to implement a
/// replicated key/value database.
class KVReplica: public Replica, public Database
{
public:
    KVReplica(
        std::shared_ptr<IPaxos1> proto,
        std::shared_ptr<util::Clock> clock,
        std::shared_ptr<oncrpc::TimeoutManager> tman,
        std::shared_ptr<Database> db);

    KVReplica(
        const std::string& replicaAddress,
        std::shared_ptr<util::Clock> clock,
        std::shared_ptr<oncrpc::SocketManager> sockman,
        std::shared_ptr<Database> db);

    // Replica overrides
    void apply(
        std::int64_t instance, const std::vector<uint8_t>& command) override;
    void leaderChanged() override;

    // Database overrides
    std::shared_ptr<Namespace> getNamespace(const std::string& name) override;
    std::unique_ptr<Transaction> beginTransaction() override;
    void commit(std::unique_ptr<Transaction>&& transaction) override;
    void flush() override;
    bool isReplicated() override { return true; }
    bool isMaster() override { return isLeader_; }
    void onMasterChange(std::function<void(bool)> cb) override;

    std::shared_ptr<Buffer> toBuffer(const std::vector<uint8_t>& vec)
    {
        auto buf = std::make_shared<Buffer>(vec.size());
        std::copy_n(vec.data(), vec.size(), buf->data());
        return buf;
    }

private:
    std::vector<std::function<void(bool)>> masterChangeCallbacks_;
};

}
}
