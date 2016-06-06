/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */


#include "nfs4fs.h"

using namespace filesys;
using namespace filesys::nfs4;
using namespace std;

NfsDirectoryIterator::NfsDirectoryIterator(
    std::shared_ptr<NfsFile> dir, std::uint64_t seek)
    : dir_(dir)
{
    if (seek == 0)
        state_ = ISDOT;
    else {
        state_ = READDIR;
        readdir(seek);
    }
}

bool NfsDirectoryIterator::valid() const
{
    return state_ != READDIR || entry_.get() != nullptr;
}

FileId NfsDirectoryIterator::fileid() const
{
    if (state_ == ISDOT)
        return FileId(dir_->attr().fileid_);
    else if (state_ == ISDOTDOT)
        return FileId(dir_->lookupp()->attr().fileid_);
    return FileId(attr_.fileid_);
}

std::string NfsDirectoryIterator::name() const
{
    if (state_ == ISDOT)
        return ".";
    else if (state_ == ISDOTDOT)
        return "..";
    return toString(entry_->name);
}

std::shared_ptr<File> NfsDirectoryIterator::file() const
{
    if (state_ == ISDOT)
        return dir_;
    else if (state_ == ISDOTDOT)
        return dir_->lookupp();

    // Note: its ok to steal the memory for filehandle and attrs here since
    // we only need filehandle once here and we have parsed the attrs
    // already
    if (file_)
        return file_;
    file_ = dir_->nfs()->find(
        move(attr_.filehandle_), move(entry_->attrs));
    return file_;
}

uint64_t NfsDirectoryIterator::seek() const
{
    if (state_ == ISDOT)
        return 0;
    else if (state_ == ISDOTDOT)
        return 0;

    return entry_->cookie;
}

void NfsDirectoryIterator::next()
{
    if (state_ == ISDOT) {
        state_ = ISDOTDOT;
        return;
    }
    if (state_ == ISDOTDOT) {
        state_ = READDIR;
        readdir(0);
        return;
    }

    file_.reset();
    auto cookie = entry_->cookie;
    auto p = std::move(entry_->nextentry);
    entry_ = std::move(p);
    if (entry_) {
        attr_.decode(entry_->attrs);
    }
    else if (!eof_) {
        readdir(cookie);
    }
}

void NfsDirectoryIterator::readdir(nfs_cookie4 cookie)
{
    if (cookie == 0) {
        std::fill_n(verf_.data(), verf_.size(), 0);
    }
    try {
        dir_->nfs()->compound(
            [this, cookie](auto& enc) {
                bitmap4 wanted;
                setSupportedAttrs(wanted);
                // We also need file handles for the directory entries
                set(wanted, FATTR4_FILEHANDLE);
                enc.putfh(dir_->fh());
                enc.readdir(cookie, verf_, 8192, 8192, wanted);
                clear(wanted, FATTR4_FILEHANDLE);
                enc.getattr(wanted);
            },
            [this](auto& dec) {
                dec.putfh();
                auto res = dec.readdir();
                verf_ = std::move(res.cookieverf);
                entry_ = std::move(res.reply.entries);
                eof_ = res.reply.eof;
                dir_->update(move(dec.getattr().obj_attributes));
            });
        if (entry_)
            attr_.decode(entry_->attrs);
    } catch (system_error&) {
        dir_.reset();
        eof_ = true;
        throw;
    }
}
