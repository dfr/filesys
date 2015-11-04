#include <system_error>

#include <fs++/filesys.h>

namespace filesys {
namespace nfs { void init(FilesystemManager* fsman); }
namespace posix { void init(FilesystemManager* fsman); }
namespace objfs { void init(FilesystemManager* fsman); }
}

using namespace filesys;

FilesystemManager::FilesystemManager()
{
    nfs::init(this);
    posix::init(this);
    objfs::init(this);
}

std::shared_ptr<File> FilesystemManager::find(const FileHandle& fh)
{
    for (auto& entry: filesystems_) {
        auto& fs = entry.second;
        if (fs->fsid() == fh.fsid)
            return fs->find(fh);
    }
    throw std::system_error(ESTALE, std::system_category());
}
