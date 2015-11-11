#include <cassert>
#include <memory>

#include <fs++/filecache.h>
#include <gmock/gmock.h>

using namespace filesys::detail;
using namespace std;
using namespace std::placeholders;
using namespace testing;

struct File
{
    File(int id) : id_(id) {}
    int fileid() const { return id_; }
    int id_;
};

struct MockCallbacks
{
    MOCK_METHOD1(update, void(shared_ptr<File>));
    MOCK_METHOD1(ctor, shared_ptr<File>(int));
};

struct FileCacheTest: public ::testing::Test
{
    static shared_ptr<File> newFile(int id)
    {
        return make_shared<File>(id);
    }

    FileCache<int, File> cache;
    MockCallbacks cb;
    function<void(shared_ptr<File>)> update = bind(&MockCallbacks::update, &cb, _1);
    function<shared_ptr<File>(int)> ctor = bind(&MockCallbacks::ctor, &cb, _1);
};

TEST_F(FileCacheTest, Basic)
{
    EXPECT_CALL(cb, ctor(1))
        .Times(1)
        .WillOnce(Invoke(newFile));
    auto f = cache.find(1, update, ctor);
    EXPECT_EQ(1, cache.size());
    EXPECT_EQ(f, cache.find(1, update, ctor));
}

TEST_F(FileCacheTest, LRU)
{
    EXPECT_CALL(cb, ctor(_))
        .Times(cache.sizeLimit() + 3)
        .WillRepeatedly(Invoke(newFile));

    // Entry 0 should expire
    for (int i = 0; i < cache.sizeLimit() + 1; i++) {
        cache.find(i, update, ctor);
    }
    EXPECT_EQ(cache.sizeLimit(), cache.size());
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
