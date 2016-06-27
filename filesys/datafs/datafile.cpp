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

DataFile::DataFile(
    shared_ptr<DataFilesystem> fs, PieceId id, shared_ptr<File> file)
    : fs_(fs),
      id_(id),
      file_(file)
{
}

shared_ptr<Filesystem> DataFile::fs()
{
    return fs_.lock();
}

FileHandle
DataFile::handle()
{
    return fs_.lock()->pieceHandle(id_);
}

bool DataFile::access(const Credential& cred, int accmode)
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

shared_ptr<Getattr> DataFile::getattr()
{
    return backingFile()->getattr();
}

void DataFile::setattr(const Credential& cred, function<void(Setattr*)> cb)
{
    backingFile()->setattr(cred, cb);
}

shared_ptr<File> DataFile::lookup(const Credential&, const string& name)
{
    throw system_error(ENOENT, system_category());
}

shared_ptr<OpenFile> DataFile::open(
    const Credential&, const string&, int, function<void(Setattr*)>)
{
    throw system_error(ENOTDIR, system_category());
}

std::shared_ptr<OpenFile> DataFile::open(const Credential& cred, int flags)
{
    return backingFile()->open(cred, flags);
}

string DataFile::readlink(const Credential&)
{
    throw system_error(EINVAL, system_category());
}

shared_ptr<File> DataFile::mkdir(
    const Credential&, const string&, function<void(Setattr*)>)
{
    throw system_error(ENOTDIR, system_category());
}

shared_ptr<File> DataFile::symlink(
    const Credential&, const string&, const string& data,
    function<void(Setattr*)>)
{
    throw system_error(ENOTDIR, system_category());
}

std::shared_ptr<File> DataFile::mkfifo(
    const Credential&, const std::string&, std::function<void(Setattr*)>)
{
    throw system_error(ENOTDIR, system_category());
}

void DataFile::remove(const Credential&, const string&)
{
    throw system_error(ENOTDIR, system_category());
}

void DataFile::rmdir(const Credential&, const string&)
{
    throw system_error(ENOTDIR, system_category());
}

void DataFile::rename(
    const Credential&, const string&, shared_ptr<File>, const string&)
{
    throw system_error(ENOTDIR, system_category());
}

void DataFile::link(const Credential&, const std::string&, std::shared_ptr<File>)
{
    throw system_error(ENOTDIR, system_category());
}

shared_ptr<DirectoryIterator> DataFile::readdir(const Credential&, uint64_t)
{
    throw system_error(ENOTDIR, system_category());
}

std::shared_ptr<Fsattr> DataFile::fsstat(const Credential& cred)
{
    return fs_.lock()->store()->root()->fsstat(cred);
}

std::shared_ptr<File> DataFile::backingFile()
{
    auto file = file_.lock();
    if (!file) {
        Credential cred{0, 0, {}, true};
        file = fs_.lock()->lookup(cred, id_);
        file_ = file;
    }
    return file;
}
