#include <fs++/filesys.h>
#include <fs++/urlparser.h>
#include <glog/logging.h>

#include "nfsfs.h"

using namespace filesys;
using namespace filesys::nfs;

NfsDirectoryIterator::NfsDirectoryIterator(
    std::shared_ptr<NfsFile> dir, std::uint64_t seek)
    : dir_(dir)
{
    readdir(seek);
}

bool NfsDirectoryIterator::valid() const
{
    return entry_.get() != nullptr;
}

FileId NfsDirectoryIterator::fileid() const
{
    return FileId(entry_->fileid);
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
        file_ = dir_->nfs()->find(
            std::move(entry_->name_handle.handle()),
            std::move(entry_->name_attributes.attributes()));
    }
    else {
        file_ = dir_->lookup(entry_->name);
    }
    return file_;
}

uint64_t NfsDirectoryIterator::seek() const
{
    return entry_->cookie;
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
    if (cookie == 0) {
        std::fill_n(verf_.data(), verf_.size(), 0);
    }
    auto res = dir_->nfs()->proto()->readdirplus(
        READDIRPLUS3args{
            dir_->fh(), cookie, verf_,
            dir_->nfs()->fsinfo().dtpref,
            dir_->nfs()->fsinfo().dtpref});
    if (res.status == NFS3_OK) {
        verf_ = std::move(res.resok().cookieverf);
        entry_ = std::move(res.resok().reply.entries);
        eof_ = res.resok().reply.eof;
    }
    else {
        dir_.reset();
        eof_ = true;
        throw std::system_error(res.status, std::system_category());
    }
}
