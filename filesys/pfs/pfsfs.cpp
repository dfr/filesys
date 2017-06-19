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

#include <cassert>
#include <system_error>

#include <rpc++/urlparser.h>
#include "pfsfs.h"

using namespace filesys;
using namespace filesys::pfs;
using namespace std;

int PfsFilesystem::nextfsid_ = 1;

static vector<string> parsePath(const string& path)
{
    vector<string> res;
    string entry;
    for (auto ch: path) {
        if (ch == '/') {
            if (entry.size() > 0) {
                res.push_back(entry);
                entry.clear();
            }
        }
        else {
            entry.push_back(ch);
        }
    }
    if (entry.size() > 0)
        res.push_back(entry);
    return res;
}

PfsFilesystem::PfsFilesystem()
{
    fsid_.resize(sizeof(uint32_t));
    *reinterpret_cast<uint32_t*>(fsid_.data()) = nextfsid_++;
}

std::shared_ptr<File>
PfsFilesystem::root()
{
    checkRoot();
    if (root_)
        return root_->checkMount();
    return
        nullptr;
}

const FilesystemId&
PfsFilesystem::fsid() const
{
    static FilesystemId nullfsid;
    return nullfsid;
}

shared_ptr<File>
PfsFilesystem::find(const FileHandle& fh)
{
    oncrpc::XdrMemory xm(
        fh.handle.data() + fsid_.size(), sizeof(std::uint64_t));
    std::uint64_t val;
    xdr(val, static_cast<oncrpc::XdrSource*>(&xm));
    auto i = idmap_.find(int(val));
    if (i == idmap_.end() || i->second.expired())
        throw system_error(ESTALE, system_category());
    return i->second.lock();
}

void
PfsFilesystem::add(const std::string& path, shared_ptr<File> mount)
{
    if (paths_.find(path) != paths_.end())
        throw system_error(EEXIST, system_category());

    checkRoot();

    vector<string> entries = parsePath(path);
    auto dir = root_;
    for (auto& entry: entries) {
        if (entry.size() > PFS_NAME_MAX)
            throw system_error(ENAMETOOLONG, system_category());
        try {
            dir = dir->find(entry);
        }
        catch (system_error& e) {
            auto newdir = make_shared<PfsFile>(
                shared_from_this(), FileId(nextid_), dir);
            idmap_[nextid_] = newdir;
            nextid_++;
            dir->add(entry, newdir);
            dir = newdir;
        }
    }
    dir->mount(mount);
    paths_[path] = dir;
}

void
PfsFilesystem::add(const std::string& path, std::shared_ptr<Filesystem> mount)
{
    subfs_.push_back(mount);
    add(path, mount->root());
}

void
PfsFilesystem::remove(const std::string& path)
{
    auto i = paths_.find(path);
    if (i == paths_.end())
        throw system_error(ENOENT, system_category());
    paths_.erase(i);
}

void
PfsFilesystem::checkRoot()
{
    if (!root_) {
        root_ = make_shared<PfsFile>(
            shared_from_this(), FileId(nextid_), nullptr);
        idmap_[nextid_] = root_;
        nextid_++;
    }
}
