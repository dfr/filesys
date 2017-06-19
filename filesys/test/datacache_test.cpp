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

#include <cassert>
#include <memory>
#include <thread>

#include <filesys/datacache.h>
#include <gmock/gmock.h>

using namespace filesys;
using namespace filesys::detail;
using namespace std;
using namespace std::placeholders;
using namespace testing;

shared_ptr<Buffer> makeBuffer(size_t len, uint8_t fill)
{
    auto b = make_shared<Buffer>(len);
    fill_n(b->data(), len, fill);
    return b;
}

struct DataCacheTest: public ::testing::Test
{
    vector<uint8_t> flatten()
    {
        uint64_t max = 0;
        cache.apply(
            [&max](auto dirty, auto start, auto end, auto buf) {
                if (end > max)
                    max = end;
            });
        vector<uint8_t> res(max, 0);
        cache.apply(
            [&res](auto dirty, auto start, auto end, auto buf) {
                copy_n(buf->data(), buf->size(), res.data() + start);
            });
        return res;
    }

    void check(vector<uint8_t> expected)
    {
        EXPECT_EQ(expected, flatten());
    }

    DataCache cache;
};

TEST_F(DataCacheTest, Basic)
{
    cache.add(DataCache::STABLE, 0, makeBuffer(10, 1));
    check({1,1,1,1,1,1,1,1,1,1});
    EXPECT_EQ(1, cache.blockCount());
}

TEST_F(DataCacheTest, LeftOverlap)
{
    cache.add(DataCache::STABLE, 0, makeBuffer(10, 1));
    cache.add(DataCache::STABLE, 5, makeBuffer(10, 2));
    check({1,1,1,1,1,2,2,2,2,2,2,2,2,2,2});
    EXPECT_EQ(2, cache.blockCount());
}

TEST_F(DataCacheTest, Split)
{
    cache.add(DataCache::STABLE, 0, makeBuffer(10, 1));
    cache.add(DataCache::STABLE, 5, makeBuffer(2, 2));
    check({1,1,1,1,1,2,2,1,1,1});
    EXPECT_EQ(3, cache.blockCount());
}

TEST_F(DataCacheTest, RightOverlap)
{
    cache.add(DataCache::STABLE, 5, makeBuffer(10, 1));
    cache.add(DataCache::STABLE, 0, makeBuffer(10, 2));
    check({2,2,2,2,2,2,2,2,2,2,1,1,1,1,1});
    EXPECT_EQ(2, cache.blockCount());
}

TEST_F(DataCacheTest, Overwrite)
{
    cache.add(DataCache::STABLE, 5, makeBuffer(2, 1));
    cache.add(DataCache::STABLE, 0, makeBuffer(10, 2));
    check({2,2,2,2,2,2,2,2,2,2});
    EXPECT_EQ(1, cache.blockCount());
}

TEST_F(DataCacheTest, Merge)
{
    cache.add(DataCache::STABLE, 0, makeBuffer(10, 1));
    cache.add(DataCache::STABLE, 5, makeBuffer(2, 2));
    auto buf = cache.get(0, 10);
    EXPECT_EQ(10, buf->size());
    EXPECT_EQ(1, cache.blockCount());
}
