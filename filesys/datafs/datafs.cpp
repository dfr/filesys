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
#include <cctype>
#include <iomanip>
#include <random>
#include <sstream>
#include <system_error>

#include <rpc++/urlparser.h>
#include <glog/logging.h>

#include "datafs.h"
#include "filesys/posix/posixfs.h"

using namespace filesys;
using namespace filesys::data;
using namespace std;

DECLARE_string(fsid);

namespace {

static inline int l2size(uint32_t val)
{
    // Special case size 0, which we use to mean 1<<64
    if (val == 0)
        return 64;
    int i = __builtin_ffs(val);
    assert(val == (1u << (i-1)));
    return i-1;
}

/// Each piece is stored within a three level directory structure. The
/// fileid is decomposed into four 16bit fields with the first three
/// naming the three directory levels and the last along with the
/// offset naming the file containing the piece data
class PiecePath
{
public:
    PiecePath(const PieceId& id)
    {
        int sz = l2size(id.size);
        auto off = sz == 64 ? 0 : (id.offset >> sz);
        path[0] = formatHex((id.fileid >> 48) & 0xffff, 4);
        path[1] = formatHex((id.fileid >> 32) & 0xffff, 4);
        path[2] = formatHex((id.fileid >> 16) & 0xffff, 4);
        path[3] = formatHex(id.fileid & 0xffff, 4) +
            '-' + to_string(sz) +
            '-' + to_string(off);
    }

    string path[4];

private:
    string formatHex(uint64_t element, int width)
    {
        ostringstream str;
        str << hex << setw(width) << setfill('0') << element;
        return str.str();
    }
};

}

static inline int fromHex(char ch)
{
    if (ch >= '0' && ch <= '9')
        return ch - '0';
    if (ch >= 'a' && ch <= 'f')
        return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F')
        return ch - 'A' + 10;
    abort();
}

DataFilesystem::DataFilesystem(shared_ptr<Filesystem> store)
    : store_(store)
{
    Credential cred(0, 0, {}, true);
    auto meta = store->root()->open(
        cred, "META", OpenFlags::RDWR|OpenFlags::CREATE,
        [](auto sattr) {
            sattr->setMode(0644);
        });

    // Initially, we just store a 128-bit random fsid. If the command
    // line flag --fsid is present, use that value for the new
    // filesystem's fsid.
    bool eof;
    auto buf = meta->read(0, 16, eof);
    if (buf->size() != 16) {
        buf = make_shared<Buffer>(16);
        if (FLAGS_fsid.size() > 0) {
            // Verify that the value is a 128-bit hex number
            bool valid = true;
            for (auto ch: FLAGS_fsid) {
                if (!std::isxdigit(ch)) {
                    valid = false;
                    break;
                }
            }
            if (!valid || FLAGS_fsid.size() != 32) {
                LOG(FATAL) << "fsid override should be 128-bit hex numner: "
                           << FLAGS_fsid;
                abort();
            }
            auto p = buf->data();
            for (int i = 0; i < 16; i++) {
                p[i] = (fromHex(FLAGS_fsid[2*i]) * 16 +
                        fromHex(FLAGS_fsid[2*i+1]));
            }
        }
        else {
            std::random_device rnd;
            uint32_t* p = reinterpret_cast<uint32_t*>(buf->data());
            p[0] = rnd();
            p[1] = rnd();
            p[2] = rnd();
            p[3] = rnd();
        }
        meta->write(0, buf);
    }

    fsid_.resize(16);
    copy_n(buf->data(), 16, fsid_.data());

    // XXX: Don't cache too many pieces - we are using select and don't
    // want to have file descriptors greater than 1023
    cache_.setCostLimit(512);
}

std::shared_ptr<File>
DataFilesystem::root()
{
    if (!root_) {
        root_ = make_shared<DataRoot>(shared_from_this());
    }
    return root_;
}

const FilesystemId&
DataFilesystem::fsid() const
{
    return fsid_;
}

shared_ptr<File>
DataFilesystem::find(const FileHandle& fh)
{
    const size_t sz = 2*sizeof(uint64_t) + sizeof(uint32_t);

    oncrpc::XdrMemory xm(
        fh.handle.data() + fsid_.size(), sz);
    uint64_t fileid, offset;
    uint32_t size;
    xdr(fileid, static_cast<oncrpc::XdrSource*>(&xm));
    xdr(offset, static_cast<oncrpc::XdrSource*>(&xm));
    xdr(size, static_cast<oncrpc::XdrSource*>(&xm));

    if (fileid == 0 && offset == 0 && size == 0)
        return root();

    Credential cred(0, 0, {}, true);
    return find(cred, PieceId{FileId(fileid), offset, size});
}

shared_ptr<File>
DataFilesystem::findPiece(const Credential& cred, const PieceId& id)
{
    VLOG(1) << "DataFilesystem::findPiece(" << id << ")";
    return find(cred, id);
}

shared_ptr<File>
DataFilesystem::createPiece(const Credential& cred, const PieceId& id)
{
    VLOG(1) << "DataFilesystem::createPiece(" << id << ")";
    open(cred, id, OpenFlags::RDWR | OpenFlags::CREATE);
    return find(cred, id);
}

void
DataFilesystem::removePiece(const Credential& cred, const PieceId& id)
{
    VLOG(1) << "DataFilesystem::removePiece(" << id << ")";
    PiecePath path(id);
    shared_ptr<File> dir = store_->root();
    shared_ptr<File> dirs[3];
    for (int i = 0; i < 3; i++) {
        try {
            dir = dir->lookup(cred, path.path[i]);
            dirs[i] = dir;
        }
        catch (system_error& e) {
            return;
        }
    }
    unique_lock<mutex> lk(mutex_);
    dir->remove(cred, path.path[3]);
    // Attempt to remove empty directories - stop when we get an error
    for (int i = 2; i >= 0; i--) {
        dir = dirs[i];
        shared_ptr<File> parent;
        if (i > 0)
            parent = dirs[i - 1];
        else
            parent = store_->root();
        try {
            parent->rmdir(cred, path.path[i]);
            VLOG(1) << "Removing at level " << i << ": " << path.path[i];
        }
        catch (system_error& e) {
            break;
        }
    }
    cache_.remove(id);
}

FileHandle
DataFilesystem::pieceHandle(const PieceId& id)
{
    const size_t sz = 2*sizeof(uint64_t) + sizeof(uint32_t);
    FileHandle fh;

    fh.version = 1;
    fh.handle.resize(fsid_.size() + sz);
    copy(fsid_.begin(), fsid_.end(), fh.handle.begin());
    oncrpc::XdrMemory xm(fh.handle.data() + fsid_.size(), sz);
    xdr(id.fileid, static_cast<oncrpc::XdrSink*>(&xm));
    xdr(id.offset, static_cast<oncrpc::XdrSink*>(&xm));
    xdr(id.size, static_cast<oncrpc::XdrSink*>(&xm));
    return fh;
}

bool
DataFilesystem::exists(const PieceId& id)
{
    // Root directory has piece id {0,0,0}
    if (id.fileid == 0 && id.offset == 0 && id.size == 0)
        return true;

    // Look up in our cache first so that we can reduce the number of open
    // attempts for pieces we know exist
    Credential cred(0, 0, {}, true);
    unique_lock<mutex> lk(mutex_);
    if (!cache_.contains(id)) {
        lk.unlock();
        try {
            lookup(cred, id);
        }
        catch (system_error&) {
            return false;
        }
    }
    return true;
}

shared_ptr<DataFile> DataFilesystem::find(
    const Credential& cred, const PieceId& id)
{
    unique_lock<mutex> lock(mutex_);
    return cache_.find(
        id,
        [](auto) {
        },
        [this, &cred](const PieceId& id) {
            // Attempt to open the file to verify that we actually
            // have this piece. If we don't have it, lookup will throw a
            // suitable exception.
            return make_shared<DataFile>(
                shared_from_this(), id, lookup(cred, id));
        });
}

std::shared_ptr<File>
DataFilesystem::lookup(const Credential& cred, const PieceId& id)
{
    PiecePath path(id);

    shared_ptr<File> dir = store_->root();
    for (int i = 0; i < 3; i++) {
        dir = dir->lookup(cred, path.path[i]);
    }
    return dir->lookup(cred, path.path[3]);
}

std::shared_ptr<OpenFile>
DataFilesystem::open(const Credential& cred, const PieceId& id, int flags)
{
    PiecePath path(id);
    bool create = (flags & OpenFlags::CREATE) != 0;

    unique_lock<mutex> lk(mutex_);
    shared_ptr<File> dir = store_->root();
    for (int i = 0; i < 3; i++) {
        try {
            dir = dir->lookup(cred, path.path[i]);
        }
        catch (system_error& e) {
            if (create && e.code().value() == ENOENT) {
                VLOG(1) << "Creating level " << i << ": " << path.path[i];
                dir = dir->mkdir(
                    cred, path.path[i],
                    [](auto sattr) {
                        sattr->setMode(0755);
                    });
            }
            else {
                throw;
            }
        }
    }
    // XXX: wrap the backing store OpenFile to validate i/o bounds
    VLOG(1) << "Opening file: " << path.path[3];
    return dir->open(cred, path.path[3], flags,
                     [](auto sattr){ sattr->setMode(0644); });
}

shared_ptr<Filesystem>
DataFilesystemFactory::mount(const string& url)
{
    oncrpc::UrlParser p(url);
    return make_shared<DataFilesystem>(
        make_shared<posix::PosixFilesystem>(p.path));
};

void filesys::data::init(FilesystemManager* fsman)
{
    oncrpc::UrlParser::addPathbasedScheme("datafs");
    fsman->add(make_shared<DataFilesystemFactory>());
}
