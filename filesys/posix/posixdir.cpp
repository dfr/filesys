#include <fcntl.h>
#include <system_error>
#include <unistd.h>

#include "posixfs.h"

using namespace filesys;
using namespace filesys::posix;
using namespace std;

PosixDirectoryIterator::PosixDirectoryIterator(
    shared_ptr<PosixFilesystem> fs, shared_ptr<PosixFile> parent)
    : fs_(fs),
      parent_(parent),
      dir_(::fdopendir(::openat(parent->fd(), ".", O_RDONLY, 0)))
{
    if (dir_)
        next_ = ::readdir(dir_);
    else
        next_ = nullptr;
}

PosixDirectoryIterator::~PosixDirectoryIterator()
{
    if (dir_)
        ::closedir(dir_);
}

bool PosixDirectoryIterator::valid() const
{
    return next_ != nullptr;
}

uint64_t PosixDirectoryIterator::fileid() const
{
    return next_->d_ino;
}

string PosixDirectoryIterator::name() const
{
    return next_->d_name;
}

shared_ptr<File> PosixDirectoryIterator::file() const
{
    int fd;
    if (next_->d_type == DT_LNK) {
        // We can't open symlinks
        fd = -1;
    }
    else {
        fd = ::openat(parent_->fd(), next_->d_name, O_RDONLY, 0);
        if (fd < 0)
            throw system_error(errno, system_category());
    }
    return fs_->find(parent_, next_->d_name, next_->d_ino, fd);
}

void PosixDirectoryIterator::next()
{
    if (dir_)
        next_ = ::readdir(dir_);
}
