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

#include <thread>
#include <unordered_set>

#include <filesys/fstests.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include "filesys/objfs/objfs.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace keyval;
using namespace std;

class ObjfsTest
{
public:
    ObjfsTest()
    {
        Credential cred(0, 0, {}, true);
        clock_ = make_shared<util::MockClock>();
        fs_ = make_shared<ObjFilesystem>(make_memdb(), nullptr, clock_);
        blockSize_ = fs_->blockSize();
        fs_->root()->setattr(cred, setMode777);
    }

    void setCred(const Credential&) {}

    shared_ptr<util::MockClock> clock_;
    shared_ptr<ObjFilesystem> fs_;
    size_t blockSize_;
};

INSTANTIATE_TYPED_TEST_CASE_P(ObjfsTest, FilesystemTest, ObjfsTest);

class ObjfsTestExtra: public ObjfsTest, public ::testing::Test
{
};

TEST_F(ObjfsTestExtra, MultiThread)
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
}

int main(int argc, char **argv) {
    gflags::AllowCommandLineReparsing();
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging(argv[0]);
    return RUN_ALL_TESTS();
}
