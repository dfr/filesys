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

#include <atomic>
#include <cassert>
#include <memory>
#include <thread>

#include <util/lrucache.h>
#include <gmock/gmock.h>

using namespace util;
using namespace std;
using namespace std::placeholders;
using namespace testing;

struct File
{
    File(int id) : id_(id) {}
    int cost() const { return 1; }
    int fileid() const { return id_; }
    int id_;
};

struct MockCallbacks
{
    MOCK_METHOD1(update, void(shared_ptr<File>));
    MOCK_METHOD1(ctor, shared_ptr<File>(int));
};

struct LRUCacheTest: public ::testing::Test
{
    static shared_ptr<File> newFile(int id)
    {
        return make_shared<File>(id);
    }

    LRUCache<int, File> cache;
    MockCallbacks cb;
    function<void(shared_ptr<File>)> update = bind(&MockCallbacks::update, &cb, _1);
    function<shared_ptr<File>(int)> ctor = bind(&MockCallbacks::ctor, &cb, _1);
};

TEST_F(LRUCacheTest, Basic)
{
    EXPECT_CALL(cb, ctor(1))
        .Times(1)
        .WillOnce(Invoke(newFile));
    EXPECT_CALL(cb, update(_))
        .Times(1);
    auto f = cache.find(1, update, ctor);
    EXPECT_EQ(1, cache.totalCost());
    EXPECT_EQ(f, cache.find(1, update, ctor));
}

TEST_F(LRUCacheTest, LRU)
{
    EXPECT_CALL(cb, ctor(_))
        .Times(cache.costLimit() + 3)
        .WillRepeatedly(Invoke(newFile));

    // Entry 0 should expire
    for (int i = 0; i < cache.costLimit() + 1; i++) {
        cache.find(i, update, ctor);
    }
    EXPECT_EQ(cache.costLimit(), cache.totalCost());
    EXPECT_EQ(false, cache.contains(0));

    // We should re-create it here and entry 1 should expire
    cache.find(0, update, ctor);
    EXPECT_EQ(true, cache.contains(0));
    EXPECT_EQ(false, cache.contains(1));

    // Update entry 2 and verify that it doesn't expire when we re-create
    // entry 1
    EXPECT_CALL(cb, update(_));
    cache.find(2, update, ctor);
    cache.find(1, update, ctor);
    EXPECT_EQ(true, cache.contains(2));
    EXPECT_EQ(false, cache.contains(3));
}

TEST_F(LRUCacheTest, Busy)
{
    EXPECT_CALL(cb, ctor(_))
        .Times(cache.costLimit() + 1)
        .WillRepeatedly(Invoke(newFile));

    // Entry 1 should expire since entry 0 will be busy
    auto e0 = cache.find(0, update, ctor);
    for (int i = 1; i < cache.costLimit() + 1; i++) {
        cache.find(i, update, ctor);
    }
    EXPECT_EQ(cache.costLimit(), cache.totalCost());
    EXPECT_EQ(true, cache.contains(0));
    EXPECT_EQ(false, cache.contains(1));
}

TEST_F(LRUCacheTest, Multithread)
{
    EXPECT_CALL(cb, ctor(_))
        .Times(100*100)
        .WillRepeatedly(Invoke(newFile));

    vector<thread> threads;
    atomic<int> counter(0);
    for (int i = 0; i < 100; i++) {
        threads.emplace_back([i, this, &counter]() {
            for (int j = 0; j < 100; j++)
                cache.find(counter++, update, ctor);
        });
    }
    for (auto& t: threads)
        t.join();
    EXPECT_EQ(100*100, counter);
}
