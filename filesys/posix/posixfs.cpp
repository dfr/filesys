#include <iomanip>
#include <sstream>

#include <fcntl.h>
#include <sys/stat.h>

#include <fs++/filesys.h>
#include <fs++/urlparser.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "posixfs.h"

using namespace filesys;
using namespace filesys::posix;
using namespace std;

PosixFilesystem::PosixFilesystem(const std::string& path)
{
    rootfd_ = ::open(path.size() > 0 ? path.c_str() : ".", O_RDONLY);
    if (rootfd_ < 0)
        throw system_error(errno, system_category());
    struct ::stat st;
    ::fstat(rootfd_, &st);
    rootid_ = FileId(st.st_ino);
    struct ::statfs stfs;
    if (::fstatfs(rootfd_, &stfs) < 0)
        throw system_error(errno, system_category());
    fsid_.resize(sizeof(stfs.f_fsid));
    copy_n(reinterpret_cast<const uint8_t*>(&stfs.f_fsid),
           sizeof(stfs.f_fsid), fsid_.data());
}

shared_ptr<File>
PosixFilesystem::root()
{
    return find(nullptr, "/", rootid_, ::dup(rootfd_));
}

const FilesystemId&
PosixFilesystem::fsid() const
{
    return fsid_;
}

shared_ptr<File>
PosixFilesystem::find(const FileHandle& fh)
{
    // To export posix file systems, we need a database mapping fileids to
    // parent directories
    throw system_error(ESTALE, system_category());
}

std::shared_ptr<PosixFile>
PosixFilesystem::find(
    std::shared_ptr<PosixFile> parent, const std::string& name, int fd)
{
    struct ::stat st;
    ::fstat(fd, &st);
    return find(parent, name, FileId(st.st_ino), fd);
}

shared_ptr<PosixFile>
PosixFilesystem::find(
    std::shared_ptr<PosixFile> parent, const std::string& name,
    FileId fileid, int fd)
{
    return cache_.find(
        fileid,
        [fd](auto) {
            ::close(fd);
        },
        [&](uint64_t id) {
            return make_shared<PosixFile>(
                shared_from_this(), parent, name, id, fd);
        });
}

void
PosixFilesystem::remove(FileId id)
{
    cache_.remove(id);
}

pair<shared_ptr<Filesystem>, string>
PosixFilesystemFactory::mount(FilesystemManager* fsman, const string& url)
{
    UrlParser p(url);
    return make_pair(fsman->mount<PosixFilesystem>(p.path, p.path), ".");
};

void filesys::posix::init(FilesystemManager* fsman)
{
    fsman->add(make_shared<PosixFilesystemFactory>());
}
