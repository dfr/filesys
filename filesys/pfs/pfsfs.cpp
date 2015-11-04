#include <cassert>
#include <system_error>

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
    return root_;
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
    assert(fh.fsid == fsid_);
    auto i = idmap_.find(int(*reinterpret_cast<const FileId*>(fh.handle.data())));
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
    assert(entries.size() > 0);

    auto dir = root_;
    for (auto& entry: entries) {
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
