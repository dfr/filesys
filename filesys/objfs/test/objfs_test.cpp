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
        fs_ = make_shared<ObjFilesystem>("testdb");
        blockSize_ = fs_->blockSize();
        fs_->root()->setattr(cred, setMode777);
    }

    void setCred(const Credential&) {}

    shared_ptr<ObjFilesystem> fs_;
    size_t blockSize_;
};

INSTANTIATE_TYPED_TEST_CASE_P(ObjfsTest, FilesystemTest, ObjfsTest);
