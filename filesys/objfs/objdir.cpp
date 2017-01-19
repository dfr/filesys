/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
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
