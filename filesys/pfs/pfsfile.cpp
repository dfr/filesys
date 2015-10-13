#include <cassert>
#include <system_error>

#include "pfsfs.h"

using namespace filesys;
using namespace filesys::pfs;
using namespace std;

PfsFile::PfsFile(
    shared_ptr<PfsFilesystem> fs, int fileid, shared_ptr<PfsFile> parent)
    : fs_(fs),
      fileid_(fileid),
      ctime_(chrono::system_clock::now()),
      parent_(parent)
{
}

shared_ptr<Filesystem> PfsFile::fs()
{
    return fs_.lock();
}

shared_ptr<Getattr> PfsFile::getattr()
{
    return make_shared<PfsGetattr>(fileid_, ctime_);
}

void PfsFile::setattr(function<void(Setattr*)> cb)
{
    throw system_error(EROFS, system_category());
}

shared_ptr<File> PfsFile::lookup(const string& name)
{
    if (name == ".") {
        return shared_from_this();
    }
    if (name == "..") {
        if (parent_)
            return parent_;
        else
            return shared_from_this();
    }
    return find(name)->checkMount();
}

shared_ptr<File> PfsFile::open(
    const string&, int, function<void(Setattr*)>)
{
    throw system_error(EISDIR, system_category());
}

void PfsFile::close()
{
    throw system_error(EISDIR, system_category());
}

void PfsFile::commit()
{
    throw system_error(EISDIR, system_category());
}

string PfsFile::readlink()
{
    throw system_error(EISDIR, system_category());
}

std::shared_ptr<oncrpc::Buffer> PfsFile::read(uint64_t, uint32_t, bool&)
{
    throw system_error(EISDIR, system_category());
}

uint32_t PfsFile::write(uint64_t, std::shared_ptr<oncrpc::Buffer>)
{
    throw system_error(EISDIR, system_category());
}

shared_ptr<File> PfsFile::mkdir(
    const string& name, function<void(Setattr*)> cb)
{
    throw system_error(EROFS, system_category());
}

shared_ptr<File> PfsFile::symlink(
    const string& name, const string& data,
    function<void(Setattr*)> cb)
{
    throw system_error(EROFS, system_category());
}

std::shared_ptr<File> PfsFile::mkfifo(
    const std::string& name, std::function<void(Setattr*)> cb)
{
    throw system_error(EROFS, system_category());
}

void PfsFile::remove(const string& name)
{
    throw system_error(EROFS, system_category());
}

void PfsFile::rmdir(const string& name)
{
    throw system_error(EROFS, system_category());
}

void PfsFile::rename(
    const string& toName, shared_ptr<File> fromDir, const string& fromName)
{
    throw system_error(EROFS, system_category());
}

void PfsFile::link(const std::string& name, std::shared_ptr<File> file)
{
    throw system_error(EROFS, system_category());
}

shared_ptr<DirectoryIterator> PfsFile::readdir()
{
    return make_shared<PfsDirectoryIterator>(entries_);
}

std::shared_ptr<Fsattr> PfsFile::fsstat()
{
    return make_shared<PfsFsattr>();
}

shared_ptr<PfsFile> PfsFile::find(const string& name)
{
    auto i = entries_.find(name);
    if (i == entries_.end())
        throw system_error(ENOENT, system_category());
    auto p = i->second;
    if (p.expired()) {
        entries_.erase(name);
        throw system_error(ENOENT, system_category());
    }
    return p.lock();
}
