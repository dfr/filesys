#include <cassert>
#include <system_error>

#include "pfsfs.h"

using namespace filesys;
using namespace filesys::pfs;
using namespace std;

PfsFile::PfsFile(int fileid, shared_ptr<PfsFile> parent)
    : fileid_(fileid),
      ctime_(std::chrono::system_clock::now()),
      parent_(parent)
{
}

shared_ptr<Getattr> PfsFile::getattr()
{
    return make_shared<PfsGetattr>(fileid_, ctime_);
}

void PfsFile::setattr(std::function<void(Setattr*)> cb)
{
    throw system_error(EROFS, system_category());
}

shared_ptr<File> PfsFile::lookup(const string& name)
{
    return find(name)->checkMount();
}

shared_ptr<File> PfsFile::open(
    const string&, int, std::function<void(Setattr*)>)
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

vector<uint8_t> PfsFile::read(uint64_t, uint32_t, bool&)
{
    throw system_error(EISDIR, system_category());
}

uint32_t PfsFile::write(uint64_t, const vector<uint8_t>&)
{
    throw system_error(EISDIR, system_category());
}

std::shared_ptr<File> PfsFile::mkdir(
    const std::string& name, std::function<void(Setattr*)> cb)
{
    throw system_error(EROFS, system_category());
}

shared_ptr<DirectoryIterator> PfsFile::readdir()
{
    return make_shared<PfsDirectoryIterator>(entries_);
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
