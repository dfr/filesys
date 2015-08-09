#include <fs++/filesys.h>
#include <fs++/nfsfs.h>
#include <fs++/urlparser.h>
#include <glog/logging.h>

#include "src/nfs/nfsfs.h"

using namespace filesys;
using namespace filesys::nfs;

NfsDirectoryIterator::NfsDirectoryIterator(std::shared_ptr<NfsFile> dir)
    : dir_(dir)
{
    readdir(0);
}

bool NfsDirectoryIterator::valid() const
{
    return entry_.get() != nullptr;
}

std::uint64_t NfsDirectoryIterator::fileid() const
{
    return entry_->fileid;
}

std::string NfsDirectoryIterator::name() const
{
    return entry_->name;
}

std::shared_ptr<File> NfsDirectoryIterator::file() const
{
    if (file_)
        return file_;
    if (entry_->name_handle.handle_follows &&
        entry_->name_attributes.attributes_follow) {
        file_ = dir_->fs()->find(
            std::move(entry_->name_handle.handle()),
            std::move(entry_->name_attributes.attributes()));
    }
    else {
        file_ = dir_->lookup(entry_->name);
    }
    return file_;
}

void NfsDirectoryIterator::next()
{
    file_.reset();
    auto cookie = entry_->cookie;
    auto p = std::move(entry_->nextentry);
    entry_ = std::move(p);
    if (!entry_ && !eof_) {
        readdir(cookie);
    }
}

void NfsDirectoryIterator::readdir(cookie3 cookie)
{
    auto res = dir_->fs()->readdirplus(
        READDIRPLUS3args{dir_->fh(), cookie, verf_, 2048, 8192});
    if (res.status == NFS3_OK) {
        verf_ = std::move(res.resok().cookieverf);
        entry_ = std::move(res.resok().reply.entries);
        eof_ = res.resok().reply.eof;
    }
    else {
        dir_.reset();
        verf_ = {};
        eof_ = true;
        throw std::system_error(res.status, std::system_category());
    }
}
