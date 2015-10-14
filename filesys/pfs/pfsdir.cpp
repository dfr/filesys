
#include "pfsfs.h"

using namespace filesys;
using namespace filesys::pfs;
using namespace std;

PfsDirectoryIterator::PfsDirectoryIterator(
    const std::map<std::string, std::weak_ptr<PfsFile>>& entries)
    : entries_(entries),
      p_(entries_.begin())
{
    skipExpired();
}

bool PfsDirectoryIterator::valid() const
{
    return p_ != entries_.end();
}

FileId PfsDirectoryIterator::fileid() const
{
    return p_->second.lock()->fileid();
}

std::string PfsDirectoryIterator::name() const
{
    return p_->first;
}

std::shared_ptr<File> PfsDirectoryIterator::file() const
{
    return p_->second.lock()->checkMount();
}

void PfsDirectoryIterator::next()
{
    ++p_;
    skipExpired();
}

void PfsDirectoryIterator::skipExpired()
{
    while (p_ != entries_.end() && p_->second.expired())
        ++p_;
}
