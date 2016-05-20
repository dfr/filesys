#include <system_error>

#include <fs++/filesys.h>
#include <glog/logging.h>

namespace filesys {
namespace nfs3 { void init(FilesystemManager* fsman); }
namespace nfs4 { void init(FilesystemManager* fsman); }
namespace posix { void init(FilesystemManager* fsman); }
namespace objfs { void init(FilesystemManager* fsman); }
namespace distfs { void init(FilesystemManager* fsman); }
namespace data { void init(FilesystemManager* fsman); }
}

using namespace filesys;

FilesystemManager::FilesystemManager()
{
    //nfs3::init(this);
    nfs4::init(this);
    posix::init(this);
    objfs::init(this);
    distfs::init(this);
    data::init(this);
}

std::shared_ptr<File> FilesystemManager::find(const FileHandle& fh)
{
    // The initial segment of the handle should match the fsid
    for (auto& entry: filesystems_) {
        auto& fs = entry.second;
        auto& fsid = fs->fsid();
        if (equal(fsid.begin(), fsid.end(),
            fh.handle.begin(), fh.handle.begin() + fsid.size()))
            return fs->find(fh);
    }
    throw std::system_error(ESTALE, std::system_category());
}
