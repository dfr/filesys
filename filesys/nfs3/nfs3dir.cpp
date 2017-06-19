/*-
 * Copyright (c) 2016-present Doug Rabson
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <filesys/filesys.h>
#include <rpc++/urlparser.h>
#include <glog/logging.h>

#include "nfs3fs.h"

using namespace filesys;
using namespace filesys::nfs3;

NfsDirectoryIterator::NfsDirectoryIterator(
    const Credential& cred, std::shared_ptr<NfsFile> dir, std::uint64_t seek)
    : cred_(cred),
      dir_(dir)
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
        file_ = dir_->lookup(cred_, entry_->name);
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
        dir_->update(res.resok().dir_attributes);
        verf_ = std::move(res.resok().cookieverf);
        entry_ = std::move(res.resok().reply.entries);
        eof_ = res.resok().reply.eof;
    }
    else {
        dir_->update(res.resfail().dir_attributes);
        dir_.reset();
        eof_ = true;
        throw std::system_error(res.status, std::system_category());
    }
}
