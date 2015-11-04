#include <cctype>
#include <system_error>

#include <fs++/filesys.h>

#include "cli/fscli.h"

using namespace filesys;
using namespace std;

CommandState::CommandState(shared_ptr<File> root)
    : root_(root),
      cwd_(root)
{
}

static int parseHexDigit(char ch)
{
    if (ch >= '0' && ch <= '9')
        return ch - '0';
    if (ch >= 'a' && ch <= 'f')
        return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F')
        return ch - 'A' + 10;
    throw system_error(ENOENT, system_category());
}

static vector<uint8_t> parseByteArray(const string& s)
{
    vector<uint8_t> res;
    uint8_t b;

    if (s.size() & 1)
        throw system_error(ENOENT, system_category());
    for (auto i = 0; i < int(s.size()); i++) {
        auto n = parseHexDigit(s[i]);
        if (i & 1) {
            b |= n;
            res.push_back(b);
        }
        else {
            b = n << 4;
        }
    }
    return res;
}

static shared_ptr<File> lookupfh(const string& s)
{
    auto i = s.find('/');
    if (i == string::npos || s.find('/', i + 1) != string::npos)
        throw system_error(ENOENT, system_category());
    auto fsid = parseByteArray(s.substr(3, i - 3));
    auto handle = parseByteArray(s.substr(i + 1));
    FileHandle fh{1, fsid, handle};
    return FilesystemManager::instance().find(fh);
}

shared_ptr<File> CommandState::lookup(const string& name)
{
    if (name.substr(0, 3) == "FH:") {
        return lookupfh(name);
    }
    auto p = resolvepath(name);
    return p.first->lookup(p.second);
}

shared_ptr<File> CommandState::open(const string& name, int flags, int mode)
{
    auto p = resolvepath(name);
    return p.first->open(
        p.second, flags, [mode](Setattr* sattr){ sattr->setMode(mode); });
}

shared_ptr<File> CommandState::mkdir(const string& name, int mode)
{
    auto p = resolvepath(name);
    return p.first->mkdir(
        p.second, [mode](Setattr* sattr){ sattr->setMode(mode); });
}

shared_ptr<File> CommandState::symlink(const string& name, const string& path)
{
    auto p = resolvepath(name);
    return p.first->symlink(
        p.second, path, [](Setattr* sattr){ sattr->setMode(0777); });
}

std::shared_ptr<File> CommandState::mkfifo(const std::string& name)
{
    auto p = resolvepath(name);
    return p.first->mkfifo(
        p.second, [](Setattr* sattr){ sattr->setMode(0666); });
}

void CommandState::remove(const std::string& name)
{
    auto p = resolvepath(name, false);
    p.first->remove(p.second);
}

void CommandState::rmdir(const std::string& name)
{
    auto p = resolvepath(name);
    p.first->rmdir(p.second);
}

void CommandState::chdir(shared_ptr<File> dir)
{
    cwd_ = dir;
}

std::pair<std::shared_ptr<File>, std::string>
CommandState::resolvepath(const std::string& name, bool follow)
{
    shared_ptr<File> f;
    auto path = parsePath(name);
    if (name[0] == '/') {
        f = root_;
    }
    else {
        f = cwd_;
    }
restart:
    if (path.size() == 0) {
        return make_pair(f, ".");
    }
    auto leafEntry = path.back();
    path.pop_back();
    while (path.size() > 0) {
        auto entry = path.front();
        path.pop_front();
        f = f->lookup(entry);
        if (f->getattr()->type() == FileType::SYMLINK) {
            auto dest = f->readlink();
            auto newpath = parsePath(dest);
            if (dest[0] == '/')
                f = root_;
            for (auto& entry: path)
                newpath.push_back(entry);
            path = newpath;
        }
    }
    try {
        // If the leaf exists, check for symbolic links
        auto leaf = f->lookup(leafEntry);
        if (follow && leaf->getattr()->type() == FileType::SYMLINK) {
            auto dest = leaf->readlink();
            if (dest[0] == '/')
                f = root_;
            path = parsePath(dest);
            goto restart;
        }
    }
    catch (system_error& e) {
        // Ignore
    }
    return make_pair(f, leafEntry);
}
