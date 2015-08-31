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
    rootid_ = st.st_ino;
}

shared_ptr<File>
PosixFilesystem::root()
{
    return find(nullptr, "/", rootid_, ::dup(rootfd_));
}

std::shared_ptr<PosixFile>
PosixFilesystem::find(
    std::shared_ptr<PosixFile> parent, const std::string& name, int fd)
{
    struct ::stat st;
    ::fstat(fd, &st);
    return find(parent, name, st.st_ino, fd);
}

shared_ptr<PosixFile>
PosixFilesystem::find(
    std::shared_ptr<PosixFile> parent, const std::string& name,
    std::uint64_t id, int fd)
{
    auto i = cache_.find(id);
    if (i != cache_.end()) {
        VLOG(2) << "cache hit for fileid: " << id << ", closing fd: " << fd;
        ::close(fd);
        auto p = i->second;
        lru_.splice(lru_.begin(), lru_, p);
        return *p;
    }
    else {
        // Expire old entries if the cache is full
        if (cache_.size() == maxCache_) {
            auto oldest = lru_.back();
            VLOG(2) << "expiring fileid: " << oldest->fileid();
            cache_.erase(oldest->fileid());
            lru_.pop_back();
        }
        VLOG(2) << "adding fileid: " << id << ", fd: " << fd;
        auto file = make_shared<PosixFile>(
            shared_from_this(), parent, name, id, fd);
        auto p = lru_.insert(lru_.begin(), file);
        cache_[id] = p;
        return file;
    }
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
