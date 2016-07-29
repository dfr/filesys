/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <random>

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
        clock_ = make_shared<detail::MockClock>();

        // Create a scratch metadata filesystem to 'export'
        mds_ = make_shared<DistFilesystem>(keyval::make_memdb(), "", clock_);
        fsman_.mount("/", mds_);
        Credential cred(0, 0, {}, true);
        mds_->root()->setattr(cred, setMode777);
        blockSize_ = 4096;

        for (int i = 0; i < 5; i++) {
            auto ds = make_shared<DataFilesystem>(
                make_shared<ObjFilesystem>(
                    keyval::make_memdb(), clock_));
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

    shared_ptr<detail::MockClock> clock_;
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
