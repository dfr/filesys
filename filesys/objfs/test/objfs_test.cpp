#include <thread>
#include <unordered_set>

#include <fs++/fstests.h>
#include "filesys/objfs/objfs.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace std;

class ObjfsTest
{
public:
    ObjfsTest()
    {
        Credential cred(0, 0, {}, true);
        system("rm -rf ./testdb");
        clock_ = make_shared<detail::MockClock>();
        fs_ = make_shared<ObjFilesystem>("testdb", clock_);
        blockSize_ = fs_->blockSize();
        fs_->root()->setattr(cred, setMode777);
    }

    void setCred(const Credential&) {}

    shared_ptr<detail::MockClock> clock_;
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
