/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#include "filesys/pfs/pfsfs.h"
#include <gtest/gtest.h>

using namespace filesys;
using namespace filesys::pfs;
using namespace std;

class PfsTest: public ::testing::Test
{
public:
    PfsTest()
    {
        fs = make_shared<PfsFilesystem>();
    }

    shared_ptr<File> lookup(vector<string> path)
    {
        auto dir = fs->root();
        for (auto& entry: path) {
            dir = dir->lookup(cred, entry);
        }
        return dir;
    }

    shared_ptr<PfsFilesystem> fs, subfs;
    Credential cred;
};

TEST_F(PfsTest, AddPath)
{
    fs->add("foo/bar/baz", shared_ptr<File>(nullptr));
    fs->add("foo/bar/qux", shared_ptr<File>(nullptr));
    lookup({"foo", "bar", "baz"});
    lookup({"foo", "bar", "qux"});
}

TEST_F(PfsTest, RemovePath)
{
    fs->add("foo/bar/baz", shared_ptr<File>(nullptr));
    fs->add("foo/bar/qux", shared_ptr<File>(nullptr));
    fs->remove("foo/bar/baz");
    lookup({"foo", "bar", "qux"});
    EXPECT_THROW(
        lookup({"foo", "bar", "baz"}),
        system_error);
}

TEST_F(PfsTest, Readdir)
{
    fs->add("foo/bar/baz", shared_ptr<File>(nullptr));
    fs->add("foo/bar/qux", shared_ptr<File>(nullptr));
    fs->add("foo/bar/foobar", shared_ptr<File>(nullptr));

    auto dir = lookup({"foo", "bar"});
    static const char* names[] = { "baz", "foobar", "qux" };
    int i = 0;
    for (auto iter = dir->readdir(cred, 0); iter->valid(); iter->next(), i++) {
        EXPECT_EQ(names[i], iter->name());
    }
}

TEST_F(PfsTest, Mount)
{
    auto m = make_shared<PfsFilesystem>();
    fs->add("foo/bar", m->root());
    EXPECT_EQ(m->root(), lookup({"foo", "bar"}));
}
