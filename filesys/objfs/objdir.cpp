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


#include "objfs.h"

using namespace filesys;
using namespace filesys::objfs;
using namespace std;

ObjDirectoryIterator::ObjDirectoryIterator(
    std::shared_ptr<ObjFilesystem> fs, FileId fileid, uint64_t seek)
    : fs_(fs),
      seek_(seek + 1),
      start_(fileid, ""),
      end_(FileId(fileid + 1), ""),
      iterator_(fs->directoriesNS()->iterator(start_, end_))
{
    // XXX: lame seek implementation
    while (seek > 0 && iterator_->valid()) {
        iterator_->next();
        seek--;
    }
    decodeEntry();
}

bool ObjDirectoryIterator::valid() const
{
    return iterator_->valid();
}

FileId ObjDirectoryIterator::fileid() const
{
    return FileId(entry_.fileid);
}

std::string ObjDirectoryIterator::name() const
{
    auto key = iterator_->key();
    return string(
        reinterpret_cast<const char*>(key->data() + sizeof(uint64_t)),
        key->size() - sizeof(uint64_t));
}

std::shared_ptr<File> ObjDirectoryIterator::file() const
{
    if (!file_)
        file_ = fs_->find(fileid());
    return file_;
}

uint64_t ObjDirectoryIterator::seek() const
{
    return seek_;
}

void ObjDirectoryIterator::next()
{
    seek_++;
    file_.reset();
    iterator_->next();
    decodeEntry();
}

void ObjDirectoryIterator::decodeEntry()
{
    if (valid()) {
        auto val = iterator_->value();
        oncrpc::XdrMemory xm(val->data(), val->size());
        xdr(entry_, static_cast<oncrpc::XdrSource*>(&xm));
    }
}
