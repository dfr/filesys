/*-
 * Copyright (c) 2016-present Doug Rabson
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <gmock/gmock.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "keyval/paxos/paxos.h"
#include "reflector.h"

using namespace keyval::paxos;
using namespace testing;

static std::vector<uint8_t> ToOpaque(const std::string& s)
{
    std::vector<uint8_t> res(s.size());
    std::copy_n(
        reinterpret_cast<const uint8_t*>(s.data()), s.size(), res.begin());
    return res;
}

static UUID MockId(int i)
{
    UUID t;
    for (auto& b: t)
        b = i;
    return t;
}

struct MockProto: public IPaxos1
{
    // IPaxos1 overrides
    MOCK_METHOD0(null, void());
    MOCK_METHOD1(identity, void(const IDENTITYargs&));
    MOCK_METHOD1(prepare, void(const PREPAREargs&));
    MOCK_METHOD1(promise, void(const PROMISEargs&));
    MOCK_METHOD1(accept, void(const ACCEPTargs&));
    MOCK_METHOD1(accepted, void(const ACCEPTargs&));
    MOCK_METHOD1(nack, void(const NACKargs&));
};

struct SaveRound
{
    SaveRound(PaxosRound& i)
        : i_(i)
    {
    }

    template <typename T>
    void operator()(const T& args) {
        i_ = args.i;
    }

    PaxosRound& i_;
};

/// Implement Replica::apply for unit tests
class MyReplica: public Replica
{
public:
    MyReplica(
        std::shared_ptr<IPaxos1> proto,
        std::shared_ptr<util::Clock> clock,
        std::shared_ptr<oncrpc::TimeoutManager> tman,
        std::shared_ptr<keyval::Database> db)
        : Replica(proto, clock, tman, db)
    {
    }

    void apply(
        std::int64_t instance, const std::vector<uint8_t>& command) override
    {
        LOG(INFO) << "Applying transaction: " << instance
                  << ": [" << command << "]";
        count++;
    }

    void leaderChanged() override
    {
        LOG(INFO) << "Leader changed: "
                  << (isLeader_ ? "leader" : "follower");
    }

    int count = 0;
};

/// This fixture aims to test the behaviour of a single replica in
/// isolation
struct ReplicaTest: public ::testing::Test
{
    ReplicaTest()
        : proto(std::make_shared<MockProto>()),
          clock(std::make_shared<util::MockClock>()),
          tman(std::make_shared<oncrpc::TimeoutManager>())
    {
        EXPECT_CALL(*proto, identity(_)).Times(1);
        self = std::make_shared<MyReplica>(
            proto, clock, tman, keyval::make_memdb());
        Mock::VerifyAndClearExpectations(proto.get());
    }

    /// Simulate the existence of some number of healthy replicas
    void addReplicas(int n)
    {
        for (int i = 0; i < n; i++)
            self->identity(IDENTITYargs{MockId(1 + i), STATUS_HEALTHY, 0});
    }

    /// Advance the simulation clock and process timeouts
    void run(util::Clock::duration dur)
    {
        auto when = clock->now() + dur;
        while (tman->next() < when) {
            *clock += (tman->next() - clock->now());
            tman->update(tman->next());
        }
        *clock += when - clock->now();
    }

    std::shared_ptr<MockProto> proto;
    std::shared_ptr<util::MockClock> clock;
    std::shared_ptr<oncrpc::TimeoutManager> tman;
    std::shared_ptr<MyReplica> self;
    PaxosCommand null = {};
};

TEST_F(ReplicaTest, Identity)
{
    // Each replica starts life believing that it is healthy and not
    // the current leader
    EXPECT_EQ(1, self->peers());
    EXPECT_EQ(1, self->healthy());
    EXPECT_EQ(0, self->recovering());
    EXPECT_EQ(false, self->isLeader());

    // Make sure this replica sends an identity message within the
    // leader wait time window. It will also send a prepare since it
    // has reason to believe there has been a leader failure.
    EXPECT_CALL(*proto, prepare(_)).Times(1);
    EXPECT_CALL(*proto, identity(_)).Times(AtLeast(2));
    run(2*LEADER_WAIT_TIME);

    // Make sure the replica sees its own identity message
    self->identity(IDENTITYargs{self->uuid(), STATUS_HEALTHY, 0});

    self->identity({MockId(1), STATUS_HEALTHY, 0});
    EXPECT_EQ(2, self->peers());
    EXPECT_EQ(2, self->healthy());
    EXPECT_EQ(0, self->recovering());

    self->identity({MockId(2), STATUS_HEALTHY, 0});
    EXPECT_EQ(3, self->peers());
    EXPECT_EQ(3, self->healthy());
    EXPECT_EQ(0, self->recovering());

    self->identity({MockId(1), STATUS_RECOVERING, 0});
    EXPECT_EQ(3, self->peers());
    EXPECT_EQ(2, self->healthy());
    EXPECT_EQ(1, self->recovering());

    self->identity({MockId(1), STATUS_HEALTHY, 0});
    EXPECT_EQ(3, self->peers());
    EXPECT_EQ(3, self->healthy());
    EXPECT_EQ(0, self->recovering());
}

TEST_F(ReplicaTest, ExtendLease)
{
    // If a replica which believes it is leader doesn't execute a new
    // transaction within the LEADER_WAIT_TIME period, it should
    // attempt to execute a null transaction to extend its lease
    self->forceLeader();
    EXPECT_CALL(*proto, prepare(_)).Times(1);
    EXPECT_CALL(*proto, identity(_)).Times(1);
    run(LEADER_WAIT_TIME);
}

TEST_F(ReplicaTest, Prepare)
{
    // Verify that the replica responds to a prepare message with a
    // corresponding promise.
    PaxosRound i{2, MockId(1)};
    EXPECT_CALL(*proto, promise(
                    AllOf(Field(&PROMISEargs::instance, 1),
                          Field(&PROMISEargs::i, i))))
        .Times(1);
    self->prepare({MockId(2), 1, i});

    // Verify that subsequently, the replica responds to prepare
    // messages with round less than the preceding prepare with a nack
    // message.
    PaxosRound j{1, MockId(1)};
    EXPECT_CALL(*proto, nack(
                    AllOf(Field(&NACKargs::instance, 1),
                          Field(&NACKargs::i, i))))
        .Times(1);
    self->prepare({MockId(2), 1, i});
    self->prepare({MockId(2), 1, j});
}

TEST_F(ReplicaTest, Accept)
{
    // Verify that the replica responds to an accept message following
    // a corresponding prepare with an accepted message.
    PaxosRound i{2, MockId(1)};
    PaxosCommand c = ToOpaque("fruit");

    EXPECT_CALL(*proto, promise(
                    AllOf(Field(&PROMISEargs::instance, 1),
                          Field(&PROMISEargs::i, i))))
        .Times(1);
    EXPECT_CALL(*proto, accepted(
                    AllOf(Field(&ACCEPTargs::instance, 1),
                          Field(&ACCEPTargs::i, i),
                          Field(&ACCEPTargs::v, c))))
        .Times(1);

    self->prepare({MockId(2), 1, i});
    self->accept({MockId(2), 1, i, c});

    // Verify that a nack is sent if we repeat the vote or attempt to
    // get a vote accepted with an obsolete round
    PaxosRound j{1, MockId(1)};
    EXPECT_CALL(*proto, nack(
                    AllOf(Field(&NACKargs::instance, 1),
                          Field(&NACKargs::i, i))))
        .Times(2);

    self->accept({MockId(2), 1, i, c});
    self->accept({MockId(2), 1, j, c});
}

TEST_F(ReplicaTest, Simple)
{
    // Simple un-contested operation.
    addReplicas(4);

    // Verify that the replica sends a prepare message when it
    // receives a new command from a client
    PaxosRound i;
    PaxosCommand c = ToOpaque("lemon");

    EXPECT_CALL(*proto, prepare(Field(&PREPAREargs::instance, 1)))
        .Times(1)
        .WillOnce(Invoke(SaveRound(i)));

    self->execute(ToOpaque("lemon"));

    // After the replica receives three replies to its prepare, it
    // should send an accept message
    EXPECT_CALL(*proto, accept(
                    AllOf(Field(&ACCEPTargs::instance, 1),
                          Field(&ACCEPTargs::i, i),
                          Field(&ACCEPTargs::v, c))));
    self->promise({MockId(1), 1, i, PaxosRound(), null});
    self->promise({MockId(2), 1, i, PaxosRound(), null});
    self->promise({MockId(3), 1, i, PaxosRound(), null});

    // After receiving three replies to the accept, it should call apply
    self->accepted({MockId(1), 1, i, c});
    self->accepted({MockId(2), 1, i, c});
    self->accepted({MockId(3), 1, i, c});
    EXPECT_EQ(1, self->count);
}

TEST_F(ReplicaTest, PrepareConflict)
{
    // Check that prepare is re-sent if a replica replies that it has
    // participated in some other round of the protocol
    addReplicas(4);

    // Force our replica to be leader
    self->forceLeader();

    // Verify that the replica sends a prepare message when it
    // receives a new command from a client
    PaxosRound i;
    PaxosCommand c = ToOpaque("lemon");

    EXPECT_CALL(*proto, prepare(Field(&PREPAREargs::instance, 1)))
        .Times(1)
        .WillOnce(Invoke(SaveRound(i)));
    self->execute(ToOpaque("lemon"));

    // If the replica receives a nack message, it should respond by
    // sending a new prepare with a higher round number
    PaxosRound j{3, MockId(3)};
    EXPECT_CALL(*proto, prepare(Field(&PREPAREargs::instance, 1)))
        .Times(1)
        .WillOnce(Invoke(SaveRound(i)));
    self->nack({MockId(2), 1, j});
    EXPECT_GT(i, j);

    // After the replica receives three replies to its second prepare,
    // it should send an accept message
    EXPECT_CALL(*proto, accept(
                    AllOf(Field(&ACCEPTargs::instance, 1),
                          Field(&ACCEPTargs::i, i),
                          Field(&ACCEPTargs::v, c))));
    self->promise({MockId(1), 1, i, PaxosRound(), null});
    self->promise({MockId(2), 1, i, PaxosRound(), null});
    self->promise({MockId(3), 1, i, PaxosRound(), null});

    // If the replica receives a nack message for the accept, it
    // should respond by sending a new prepare with a higher round
    // number
    PaxosRound k{5, MockId(3)};
    EXPECT_CALL(*proto, prepare(Field(&PREPAREargs::instance, 1)))
        .Times(1)
        .WillOnce(Invoke(SaveRound(i)));
    self->nack({MockId(2), 1, k});
    EXPECT_GT(i, j);
}

TEST_F(ReplicaTest, PrepareConflict2)
{
    // Check that prepare is sent for a new instance if a replica
    // replies that it has voted in some other round of the protocol
    addReplicas(4);

    // Force our replica to be leader
    self->forceLeader();

    // Verify that the replica sends a prepare message when it
    // receives a new command from a client
    PaxosRound i;
    PaxosCommand c = ToOpaque("lemon");
    PaxosCommand c2 = ToOpaque("orange");

    EXPECT_CALL(*proto, prepare(Field(&PREPAREargs::instance, 1)))
        .Times(1)
        .WillOnce(Invoke(SaveRound(i)));
    self->execute(ToOpaque("lemon"));

    // If the replica receives a nack message, it should respond by
    // sending a new prepare with a higher round number
    PaxosRound j{3, MockId(3)};
    EXPECT_CALL(*proto, prepare(Field(&PREPAREargs::instance, 1)))
        .Times(1)
        .WillOnce(Invoke(SaveRound(i)));
    self->nack({MockId(2), 1, j});
    EXPECT_GT(i, j);

    // After the replica receives three replies to its second prepare,
    // it should send an accept message for the value the replicas
    // already voted on
    EXPECT_CALL(*proto, accept(
                    AllOf(Field(&ACCEPTargs::instance, 1),
                          Field(&ACCEPTargs::i, i),
                          Field(&ACCEPTargs::v, c2))));
    self->promise({MockId(1), 1, i, j, c2});
    self->promise({MockId(2), 1, i, j, c2});
    self->promise({MockId(3), 1, i, j, c2});

    // After receiving three replies to the accept, it should start a
    // new instance
    EXPECT_CALL(*proto, prepare(Field(&PREPAREargs::instance, 2)));
    self->accepted({MockId(1), 1, i, c2});
    self->accepted({MockId(2), 1, i, c2});
    self->accepted({MockId(3), 1, i, c2});
}

TEST_F(ReplicaTest, PrepareTimeout)
{
    // Check that prepare is re-sent if we don't get a quorum of
    // replies within a reasonable time

    addReplicas(4);

    // Verify that the replica sends a prepare message when it
    // receives a new command from a client
    PaxosRound i;
    PaxosCommand c = ToOpaque("lemon");

    EXPECT_CALL(*proto, prepare(Field(&PREPAREargs::instance, 1)))
        .Times(1)
        .WillOnce(Invoke(SaveRound(i)));
    self->execute(ToOpaque("lemon"));

    PaxosRound j;
    EXPECT_CALL(*proto, prepare(Field(&PREPAREargs::instance, 1)))
        .Times(1)
        .WillOnce(Invoke(SaveRound(j)));
    EXPECT_CALL(*proto, identity(_))
        .Times(AnyNumber());

    run(3 * LEADER_WAIT_TIME / 2);
    EXPECT_GT(j, i);
}

TEST_F(ReplicaTest, AcceptTimeout)
{
    // Check that prepare is re-sent if we don't get a quorum of
    // replies within a reasonable time

    addReplicas(4);

    // Verify that the replica sends a prepare message when it
    // receives a new command from a client
    PaxosRound i;
    PaxosCommand c = ToOpaque("lemon");

    EXPECT_CALL(*proto, prepare(Field(&PREPAREargs::instance, 1)))
        .Times(1)
        .WillOnce(Invoke(SaveRound(i)));
    self->execute(ToOpaque("lemon"));

    // After the replica receives three replies to its second prepare,
    // it should send an accept message
    EXPECT_CALL(*proto, accept(
                    AllOf(Field(&ACCEPTargs::instance, 1),
                          Field(&ACCEPTargs::i, i),
                          Field(&ACCEPTargs::v, c))));
    self->promise({MockId(1), 1, i, PaxosRound(), null});
    self->promise({MockId(2), 1, i, PaxosRound(), null});
    self->promise({MockId(3), 1, i, PaxosRound(), null});

    PaxosRound j;
    EXPECT_CALL(*proto, accept(Field(&ACCEPTargs::instance, 1)))
        .Times(1)
        .WillOnce(Invoke(SaveRound(j)));
    EXPECT_CALL(*proto, identity(_))
        .Times(AnyNumber());

    run(3 * LEADER_WAIT_TIME / 2);
    EXPECT_GT(j, i);
}

TEST_F(ReplicaTest, Recovery)
{
    // If the replica receives a quorum of accepted messages for an
    // instance number more than one greater than the last applied
    // instance, it should go into recovery mode and start a paxos
    // round to recover the value for the new instance
    addReplicas(4);
    PaxosRound i = {1, MockId(1)};
    PaxosRound j;
    PaxosCommand c = ToOpaque("lemon");

    EXPECT_CALL(*proto, prepare(Field(&PREPAREargs::instance, 1)))
        .Times(1)
        .WillOnce(Invoke(SaveRound(j)));

    EXPECT_CALL(*proto, identity(
                    Field(&IDENTITYargs::status, STATUS_RECOVERING)));

    self->accepted({MockId(1), 2, i, c});
    self->accepted({MockId(2), 2, i, c});
    self->accepted({MockId(3), 2, i, c});

    // At this point, the replica cannot be leader
    EXPECT_EQ(false, self->isLeader());

    // Feed values to the replica and verify that it goes back to
    // healthy.  In practice, the initial prepare is likely to be
    // rejected with a nack message but we can ignore that since other
    // tests cover that behaviour.
    EXPECT_CALL(*proto, accept(
                    AllOf(Field(&ACCEPTargs::instance, 1),
                          Field(&ACCEPTargs::i, j),
                          Field(&ACCEPTargs::v, c))));
    self->promise({MockId(1), 1, j, i, c});
    self->promise({MockId(2), 1, j, i, c});
    self->promise({MockId(3), 1, j, i, c});

    // The replica should report healthy after getting some accepted replies
    EXPECT_CALL(*proto, identity(
                    AllOf(Field(&IDENTITYargs::status, STATUS_HEALTHY),
                          Field(&IDENTITYargs::instance, 2))));
    self->accepted({MockId(1), 1, j, c});
    self->accepted({MockId(2), 1, j, c});
    self->accepted({MockId(3), 1, j, c});
}

/// Making sure that a set of replicas can reach consensus on a
/// sequence of commands
struct ConsensusTest: public ::testing::Test
{
    ConsensusTest()
        : clock(std::make_shared<util::MockClock>()),
          tman(std::make_shared<oncrpc::TimeoutManager>()),
          reflector(std::make_shared<Reflector>(clock, tman))
    {
        for (int i = 0; i < 5; i++)
            addReplica();
    }

    /// Add a replica to the set
    void addReplica()
    {
        auto replica = std::make_shared<MyReplica>(
            reflector, clock, tman, keyval::make_memdb());
        replicas.push_back(replica);
        reflector->add(replica);
    }

    /// Advance the simulation clock and process timeouts
    void run(util::Clock::duration dur)
    {
        auto when = clock->now() + dur;
        while (tman->next() < when) {
            *clock += (tman->next() - clock->now());
            tman->update(tman->next());
        }
        *clock += when - clock->now();
    }

    std::shared_ptr<util::MockClock> clock;
    std::shared_ptr<oncrpc::TimeoutManager> tman;
    std::shared_ptr<Reflector> reflector;
    std::vector<std::shared_ptr<Replica>> replicas;

    PaxosCommand null = {};
};

TEST_F(ConsensusTest, Leader)
{
    // If we just run the clock for 2*LEADER_WAIT_TIME, the replicas
    // will assume leader failure and elect a new one by attempting to
    // pass an empty transaction. Afterwards, exactly one replica
    // should be leader.
    run(2*LEADER_WAIT_TIME);
    int leaderCount = 0;
    for (auto replica: replicas) {
        if (replica->isLeader())
            leaderCount++;
    }
    EXPECT_EQ(1, leaderCount);
}

TEST_F(ConsensusTest, Simple)
{
    for (int i = 0; i < 10; i++) {
        replicas[0]->execute(ToOpaque("cmd " + std::to_string(i)));
        run(std::chrono::seconds(1));
    }
}

TEST_F(ConsensusTest, Catchup)
{
    for (int i = 0; i < 3; i++) {
        replicas[0]->execute(ToOpaque("cmd " + std::to_string(i)));
        run(std::chrono::seconds(1));
    }
    addReplica();
    replicas[0]->execute(ToOpaque("cmd 3"));
    run(std::chrono::seconds(1));
}
