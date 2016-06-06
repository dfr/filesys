/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <cassert>
#include <system_error>

#include "datafs.h"

using namespace filesys;
using namespace filesys::data;
using namespace std;

DataRoot::DataRoot(shared_ptr<DataFilesystem> fs)
    : fs_(fs)
{
}

shared_ptr<Filesystem> DataRoot::fs()
{
    return fs_.lock();
}

void
DataRoot::handle(FileHandle& fh)
{
    fh = fs_.lock()->pieceHandle(PieceId{FileId(0), 0, 0});
}

bool DataRoot::access(const Credential& cred, int accmode)
{
    auto attr = getattr();
    try {
        CheckAccess(0, 0, 0555, cred, accmode);
        return true;
    }
    catch (system_error&) {
        return false;
    }
}

shared_ptr<Getattr> DataRoot::getattr()
{
    return make_shared<DataRootGetattr>(fs_.lock()->store());
}

void DataRoot::setattr(const Credential&, function<void(Setattr*)>)
{
    throw system_error(EROFS, system_category());
}

shared_ptr<File> DataRoot::lookup(const Credential&, const string& name)
{
    throw system_error(ENOENT, system_category());
}

shared_ptr<OpenFile> DataRoot::open(
    const Credential&, const string&, int, function<void(Setattr*)>)
{
    throw system_error(EROFS, system_category());
}

std::shared_ptr<OpenFile> DataRoot::open(const Credential&, int flags)
{
    throw system_error(EISDIR, system_category());
}

string DataRoot::readlink(const Credential&)
{
    throw system_error(EINVAL, system_category());
}

shared_ptr<File> DataRoot::mkdir(
    const Credential&, const string&, function<void(Setattr*)>)
{
    throw system_error(EROFS, system_category());
}

shared_ptr<File> DataRoot::symlink(
    const Credential&, const string&, const string& data,
    function<void(Setattr*)>)
{
    throw system_error(EROFS, system_category());
}

std::shared_ptr<File> DataRoot::mkfifo(
    const Credential&, const std::string&, std::function<void(Setattr*)>)
{
    throw system_error(EROFS, system_category());
}

void DataRoot::remove(const Credential&, const string&)
{
    throw system_error(EROFS, system_category());
}

void DataRoot::rmdir(const Credential&, const string&)
{
    throw system_error(EROFS, system_category());
}

void DataRoot::rename(
    const Credential&, const string&, shared_ptr<File>, const string&)
{
    throw system_error(EROFS, system_category());
}

void DataRoot::link(const Credential&, const std::string&, std::shared_ptr<File>)
{
    throw system_error(EROFS, system_category());
}

shared_ptr<DirectoryIterator> DataRoot::readdir(
    const Credential& cred, uint64_t seek)
{
    return make_shared<DataDirectoryIterator>(
        cred, fs_.lock()->store(), int(seek));
}

std::shared_ptr<Fsattr> DataRoot::fsstat(const Credential& cred)
{
    return fs_.lock()->store()->root()->fsstat(cred);
}

DataDirectoryIterator::DataDirectoryIterator(
    const Credential& cred, shared_ptr<Filesystem> store, int seek)
    : cred_(cred),
      seek_(1),
      level_(0)
{
    dirs_[0] = store->root();
    iters_[0] = dirs_[0]->readdir(cred_, 0);
    level_ = 0;
    valid_ = true;
    next();

    while (seek > 0) {
        next();
        seek--;
    }
}

bool DataDirectoryIterator::valid() const
{
    return valid_;
}

FileId DataDirectoryIterator::fileid() const
{
    return iters_[3]->fileid();
}

string DataDirectoryIterator::name() const
{
    string res;
    for (int i = 0; i < 4; i++)
        res += iters_[i]->name();
    return res;
}

shared_ptr<File> DataDirectoryIterator::file() const
{
    return iters_[3]->file();
}

uint64_t DataDirectoryIterator::seek() const
{
    return seek_;
}

void DataDirectoryIterator::next()
{
    if (level_ == 3) {
        seek_++;
        iters_[3]->next();
    }
    for (;;) {
        if (!iters_[level_]->valid()) {
            if (level_ == 0) {
                // We have reached the end of the top-level
                valid_ = false;
                return;
            }
            dirs_[level_].reset();
            iters_[level_].reset();
            level_--;
            iters_[level_]->next();
            continue;
        }
        auto name = iters_[level_]->name();
        if (name == "META" || name == "." || name == "..") {
            iters_[level_]->next();
            continue;
        }
        if (level_ == 3) {
            break;
        }
        dirs_[level_ + 1] = iters_[level_]->file();
        iters_[level_ + 1] = dirs_[level_ + 1]->readdir(cred_, 0);
        level_++;
    }
}
