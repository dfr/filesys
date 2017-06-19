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

#include <condition_variable>
#include <mutex>

#include <gmock/gmock.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "keyval/paxos/paxos.h"
#include "reflector.h"
#include "timeout.h"

using namespace keyval;
using namespace keyval::paxos;
using namespace testing;

struct MockMasterChangeCallback
{
    MOCK_METHOD1(masterChange, void(bool));
};

struct KVReplicaTest: public ::testing::Test
{
    KVReplicaTest()
        : clock(std::make_shared<util::MockClock>()),
          tman(std::make_shared<MyTimeoutManager>(clock)),
          reflector(std::make_shared<Reflector>(clock, tman))
    {
        for (int i = 0; i < 5; i++)
            addReplica();
    }

    void SetUp() override
    {
        tman->start();
    }

    void TearDown() override
    {
        tman->stop();
        replicas.clear();
    }

    /// Add a replica to the set
    void addReplica()
    {
        auto db = make_memdb();
        auto replica = std::make_shared<KVReplica>(
            reflector, clock, tman, db);
        replicas.push_back(replica);
        dbs.push_back(db);
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

    std::shared_ptr<keyval::Buffer> toBuffer(const std::string& s)
    {
        return std::make_shared<keyval::Buffer>(s);
    }

    std::string toString(std::shared_ptr<keyval::Buffer> buf)
    {
        return std::string(
            reinterpret_cast<const char*>(buf->data()), buf->size());
    }

    std::shared_ptr<util::MockClock> clock;
    std::shared_ptr<MyTimeoutManager> tman;
    std::shared_ptr<Reflector> reflector;
    std::vector<std::shared_ptr<KVReplica>> replicas;
    std::vector<std::shared_ptr<Database>> dbs;

    PaxosCommand null = {};
};

TEST_F(KVReplicaTest, Single)
{
    // Write a value to replicas[0] and verify that its replicated to
    // the others
    auto trans = replicas[0]->beginTransaction();
    auto ns = replicas[0]->getNamespace("default");
    trans->put(ns, toBuffer("fruit"), toBuffer("lemon"));
    replicas[0]->commit(std::move(trans));

    // Give the timeout thread an opportunity to drain then test the
    // replicas
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    for (int i = 1; i < int(replicas.size()); i++) {
        EXPECT_EQ(std::string("lemon"), toString(ns->get(toBuffer("fruit"))));
    }
}

TEST_F(KVReplicaTest, MasterChange)
{
    using namespace std::placeholders;

    // Disable leadership elections to avoid confusing the test with
    // lease extension transactions
    for (auto replica: replicas)
        replica->disableLeaderElections();

    MockMasterChangeCallback cb;
    replicas[0]->onMasterChange(
        std::bind(&MockMasterChangeCallback::masterChange, &cb, _1));

    // All replicas start out as followers. When replicas[0] executes
    // a transaction it will become leader.
    EXPECT_CALL(cb, masterChange(true)).Times(1);
    auto trans = replicas[0]->beginTransaction();
    auto ns = replicas[0]->getNamespace("default");
    trans->put(ns, toBuffer("fruit"), toBuffer("lemon"));
    replicas[0]->commit(std::move(trans));
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    // If we execute a transaction on some other replica, replicas[0]
    // will become a follower
    EXPECT_CALL(cb, masterChange(false)).Times(1);
    trans = replicas[1]->beginTransaction();
    ns = replicas[1]->getNamespace("default");
    trans->put(ns, toBuffer("fruit"), toBuffer("orange"));
    replicas[1]->commit(std::move(trans));
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
}

TEST_F(KVReplicaTest, Multiple)
{
    // Write a series of values to replicas[0] and verify that its
    // replicated to the others
    auto ns = replicas[0]->getNamespace("default");

    constexpr int iterations = 1000;

    for (int i = 0; i < iterations; i++) {
        auto trans = replicas[0]->beginTransaction();
        trans->put(ns, toBuffer("value"), toBuffer(std::to_string(i)));
        replicas[0]->commit(std::move(trans));
    }

    // Give the timeout thread an opportunity to drain then test the
    // replicas. After draining, we can assume that all replicas are
    // up to date.
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    std::string s = std::to_string(iterations - 1);
    for (int i = 1; i < int(replicas.size()); i++) {
        EXPECT_EQ(s, toString(ns->get(toBuffer("value"))));
    }
}

TEST_F(KVReplicaTest, Catchup)
{
    // Write a series of values to replicas[0] and verify that its
    // replicated to the others
    auto ns = replicas[0]->getNamespace("default");

    constexpr int iterations = 100;

    for (int i = 0; i < iterations; i++) {
        // Disable one replica on the 20th iteration and re-enable it
        // on the 30th - it should catch up and be in sync by the end
        // of the test
        if (i == 20)
            reflector->disable(1);
        if (i == 30)
            reflector->enable(1);

        auto trans = replicas[0]->beginTransaction();
        trans->put(ns, toBuffer("value"), toBuffer(std::to_string(i)));
        replicas[0]->commit(std::move(trans));

    }

    // Give the timeout thread an opportunity to drain then test the
    // replicas. After draining, we can assume that all replicas are
    // up to date.
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    std::string s = std::to_string(iterations - 1);
    for (int i = 1; i < int(replicas.size()); i++) {
        EXPECT_EQ(s, toString(ns->get(toBuffer("value"))));
    }
}

TEST_F(KVReplicaTest, Reboot)
{
    // Write a series of values to replicas[0] and verify that its
    // replicated to the others
    auto ns = replicas[0]->getNamespace("default");

    constexpr int iterations = 100;

    for (int i = 0; i < iterations; i++) {
        // Disable one replica on the 20th iteration and replace it on
        // the 30th with a new replica backed by the original database
        // - it should catch up and be in sync by the end of the test
        if (i == 20)
            reflector->disable(1);
        if (i == 30) {
            auto newReplica = std::make_shared<KVReplica>(
                reflector, clock, tman, dbs[1]);
            reflector->set(1, newReplica);
        }

        auto trans = replicas[0]->beginTransaction();
        trans->put(ns, toBuffer("value"), toBuffer(std::to_string(i)));
        replicas[0]->commit(std::move(trans));
    }

    // Give the timeout thread an opportunity to drain then test the
    // replicas. After draining, we can assume that all replicas are
    // up to date.
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    std::string s = std::to_string(iterations - 1);
    for (int i = 1; i < int(replicas.size()); i++) {
        EXPECT_EQ(s, toString(ns->get(toBuffer("value"))));
    }
}
