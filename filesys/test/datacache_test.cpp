/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
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
