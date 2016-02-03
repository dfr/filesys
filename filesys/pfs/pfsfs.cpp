#include <cassert>
#include <system_error>

#include <fs++/urlparser.h>
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
PfsFilesystem::add(const std::string& path, shared_ptr<Filesystem> mount)
{
    if (paths_.find(path) != paths_.end())
        throw system_error(EEXIST, system_category());

    if (!root_) {
        root_ = make_shared<PfsFile>(
            shared_from_this(), FileId(nextid_), nullptr);
        idmap_[nextid_] = root_;
        nextid_++;
    }

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
PfsFilesystem::remove(const std::string& path)
{
    auto i = paths_.find(path);
    if (i == paths_.end())
        throw system_error(ENOENT, system_category());
    paths_.erase(i);
}
