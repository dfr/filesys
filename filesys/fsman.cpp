#include <fs++/filesys.h>

namespace filesys {
namespace nfs { void init(FilesystemManager* fsman); }
namespace posix { void init(FilesystemManager* fsman); }
}

using namespace filesys;

FilesystemManager::FilesystemManager()
{
    nfs::init(this);
    posix::init(this);
}
