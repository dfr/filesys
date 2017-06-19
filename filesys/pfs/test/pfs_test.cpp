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
