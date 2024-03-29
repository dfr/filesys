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

#include <random>
#include <thread>

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <filesys/filesys.h>
#include <filesys/fstests.h>

#include "filesys/distfs/distfs.h"
#include "filesys/datafs/datafs.h"

//DEFINE_int32(iosize, 65536, "maximum size for read or write requests");
DEFINE_string(realm, "", "Local krb5 realm name");
DEFINE_string(fsid, "", "Override file system identifier for new filesystems");

using namespace filesys;
using namespace filesys::data;
using namespace filesys::distfs;
using namespace filesys::objfs;
using namespace std;

class DistTestBase
{
public:
    DistTestBase()
        : fsman_(FilesystemManager::instance())
    {
        clock_ = make_shared<util::MockClock>();

        // Create a scratch metadata filesystem to 'export'
        vector<string> addrs = {};
        mds_ = make_shared<DistFilesystem>(
            keyval::make_memdb(), nullptr, addrs, clock_);
        fsman_.mount("/", mds_);
        Credential cred(0, 0, {}, true);
        mds_->root()->setattr(cred, setMode777);
        blockSize_ = 4096;

        for (int i = 0; i < 5; i++) {
            auto ds = make_shared<DataFilesystem>(
                make_shared<ObjFilesystem>(
                    keyval::make_memdb(), nullptr, clock_));
            ds_.push_back(ds);
            mds_->addDataStore(ds);
        }

        fs_ = mds_;
    }

    ~DistTestBase()
    {
    }

    void setCred(const Credential& cred)
    {
    }

    shared_ptr<util::MockClock> clock_;
    FilesystemManager& fsman_;
    shared_ptr<DistFilesystem> mds_;        // metadata filesystem
    vector<shared_ptr<DataFilesystem>> ds_; // data filesystems
    size_t blockSize_;

    shared_ptr<Filesystem> fs_;
};

INSTANTIATE_TYPED_TEST_CASE_P(DistTest, FilesystemTest, DistTestBase);

class DistTest: public DistTestBase, public ::testing::Test
{
};

TEST_F(DistTest, MultiThread)
{
    auto root = fs_->root();

    vector<thread> threads;
    for (int i = 0; i < 32; i++) {
        threads.emplace_back(
            [i, root]() {
                Credential cred(0, 0, {}, true);
                auto dir = root->mkdir(cred, "dir" + to_string(i), setMode777);
                constexpr int flags = OpenFlags::RDWR+OpenFlags::CREATE;
                for (int j = 0; j < 32; j++) {
                    shared_ptr<File> d;
                    string n;
                    if (j & 1) {
                        d = dir;
                        n = "foo";
                    }
                    else {
                        d = root;
                        n = to_string(i);
                    }
                    auto file = d->open(cred, n, flags, setMode666);
                    auto buf = make_shared<Buffer>(1024);
                    fill_n(buf->data(), 1024, i);
                    file->write(0, buf);
                }
            }
        );
    }
    for (auto& t: threads)
        t.join();
    threads.clear();

    Credential cred(0, 0, {}, true);
    auto of = root->open(
        cred, "foo", OpenFlags::RDWR+OpenFlags::CREATE, setMode666);
    for (int i = 0; i < 4; i++) {
        threads.emplace_back(
            [i, root, cred]() {
                default_random_engine rnd(i);
                uniform_int_distribution<> dist(0, 1024*1024);
                for (int j = 0; j < 64; j++) {
                    auto s = dist(rnd);
                    auto e = dist(rnd);
                    if (s == e)
                        continue;
                    if (s > e) {
                        swap(s, e);
                    }
                    VLOG(2) << "s=" << s << " e=" << e;
                    auto len = e - s;
                    auto buf = make_shared<Buffer>(len);
                    fill_n(buf->data(), len, i);
                    auto of = root->open(
                        cred, "foo", OpenFlags::RDWR, setMode666);
                    of->write(s, buf);
                }
            }
        );
    }
    for (auto& t: threads)
        t.join();
    threads.clear();
}

int main(int argc, char **argv) {
    gflags::AllowCommandLineReparsing();
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging(argv[0]);
    return RUN_ALL_TESTS();
}
