#include <fs++/filesys.h>

#include "cli/command.h"

using namespace filesys;
using namespace std;

vector<string> parsePath(const string& path)
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

CommandState::CommandState(shared_ptr<File> root)
    : root_(root),
      cwd_(root)
{
}

shared_ptr<File> CommandState::lookup(const string& name)
{
    shared_ptr<File> f;
    vector<string> path;
    if (name[0] == '/') {
        f = root_;
        path = parsePath(name.substr(1));
    }
    else {
        f = cwd_;
        path = parsePath(name);
    }
    for (auto& entry: path)
        f = f->lookup(entry);
    return f;
}

shared_ptr<File> CommandState::open(const string& name, int flags, int mode)
{
    shared_ptr<File> f;
    vector<string> path;
    if (name[0] == '/') {
        f = root_;
        path = parsePath(name.substr(1));
    }
    else {
        f = cwd_;
        path = parsePath(name);
    }
    int i = 0;
    for (auto& entry: path) {
        if (i == path.size() - 1)
            f = f->open(
                entry, flags,
                [mode](Setattr* sattr){ sattr->setMode(mode); });
        else
            f = f->lookup(entry);
        i++;
    }
    return f;
}

std::shared_ptr<File> CommandState::mkdir(const std::string& name, int mode)
{
    shared_ptr<File> f;
    vector<string> path;
    if (name[0] == '/') {
        f = root_;
        path = parsePath(name.substr(1));
    }
    else {
        f = cwd_;
        path = parsePath(name);
    }
    int i = 0;
    for (auto& entry: path) {
        if (i == path.size() - 1)
            f = f->mkdir(
                entry,
                [mode](Setattr* sattr){ sattr->setMode(mode); });
        else
            f = f->lookup(entry);
        i++;
    }
    return f;
}

void CommandState::chdir(shared_ptr<File> dir)
{
    cwd_ = dir;
}
