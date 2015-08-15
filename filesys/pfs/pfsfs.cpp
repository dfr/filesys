#include <cassert>
#include <system_error>

#include "pfsfs.h"

using namespace filesys;
using namespace filesys::pfs;
using namespace std;

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
}

std::shared_ptr<File>
PfsFilesystem::root()
{
    return root_;
}

void
PfsFilesystem::add(const std::string& path, shared_ptr<Filesystem> mount)
{
    if (paths_.find(path) != paths_.end())
        throw system_error(EEXIST, system_category());

    if (!root_) {
        root_ = make_shared<PfsFile>(shared_from_this(), nextid_++, nullptr);
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
                shared_from_this(), nextid_++, dir);
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
