/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <algorithm>
#include <sstream>

#include <filesys/filesys.h>

#include "cli/fscli.h"

using namespace std;

namespace filesys {

static string formatType(FileType type)
{
    switch (type) {
    case FileType::FILE:
        return "-";
    case FileType::DIRECTORY:
        return "d";
    case FileType::BLOCKDEV:
        return "b";
    case FileType::CHARDEV:
        return "c";
    case FileType::SYMLINK:
        return "l";
    case FileType::SOCKET:
        return "s";
    case FileType::FIFO:
        return "f";
    }
    abort();
}

static string formatMode(uint32_t mode)
{
    static string modes[] = {
        "---", "--x", "-w-", "-wx", "r--", "r-x", "rw-", "rwx"
    };
    auto res =
        modes[(mode >> 6) & 7] + modes[(mode >> 3) & 7] + modes[mode & 7];
    if (mode & ModeFlags::SETUID) {
        if (res[2] == 'x')
            res[2] = 's';
        else
            res[2] = 'S';
    }
    if (mode & ModeFlags::SETGID) {
        if (res[5] == 'x')
            res[5] = 's';
        else
            res[5] = 'S';
    }
    return res;
}

static string formatTime(chrono::system_clock::time_point time)
{
    ostringstream ss;
    time_t t = chrono::system_clock::to_time_t(time);
    auto tm = localtime(&t);
    ss << put_time(tm, "%Y-%m-%d %H:%M");
    return ss.str();
}

class LsCommand: public Command
{
public:
    const char* name() const override { return "ls"; }
    const char* help() const override
    {
        return "show the contents of a directory";
    }


    void usage() override
    {
        cout << "usage: ls <directory>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        auto& cred = state.cred();

        if (args.size() > 1) {
            usage();
            return;
        }
        TableFormatter<11, 4, 6, 6, -5, 1, 17, 1> tf(cout);
        try {
            typedef pair<string, shared_ptr<File>> entryT;
            vector<entryT> files;
            auto dir = args.size() == 0 ? state.cwd() : state.lookup(args[0]);
            if (dir->getattr()->type() == FileType::DIRECTORY) {
                for (auto iter = dir->readdir(cred, 0);
                    iter->valid(); iter->next()) {
                    auto name = iter->name();
                    //if (name == "." || name == "..")
                    //    continue;
                    files.push_back(make_pair(name, iter->file()));
                }
            }
            else {
                files.push_back(make_pair(args[0], dir));
            }
            struct cmp {
                int operator()(const entryT& a, const entryT& b) {
                    return a.first < b.first;
                }
            };
            sort(files.begin(), files.end(), cmp());
            for (auto& entry: files) {
                auto name = entry.first;
                auto f = entry.second;
                auto attr = f->getattr();
                if (attr->type() == FileType::SYMLINK) {
                    name += " -> ";
                    name += f->readlink(cred);
                }
                tf(formatType(attr->type()) + formatMode(attr->mode()),
                   attr->nlink(),
                   attr->uid(),
                   attr->gid(),
                   humanizeNumber(attr->size()),
                   " ",
                   formatTime(attr->mtime()),
                   name);
            }
        }
        catch (system_error& e) {
            cout << args[0] << ": " << e.what() << endl;
        }
    }
};

static CommandReg<LsCommand> reg;

}
