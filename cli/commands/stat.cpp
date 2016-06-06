/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <iostream>
#include <sstream>

#include <fs++/filesys.h>

#include "cli/fscli.h"

using namespace std;

namespace filesys {

static string formatType(FileType type)
{
    switch (type) {
    case FileType::FILE:
        return "FILE";
    case FileType::DIRECTORY:
        return "DIRECTORY";
    case FileType::BLOCKDEV:
        return "BLOCKDEV";
    case FileType::CHARDEV:
        return "CHARDEV";
    case FileType::SYMLINK:
        return "SYMLINK";
    case FileType::SOCKET:
        return "SOCKET";
    case FileType::FIFO:
        return "FIFO";
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

class StatCommand: public Command
{
public:
    const char* name() const override { return "stat"; }

    const char* help() const override
    {
        return "change file mode";
    }

    void usage() override
    {
        cout << "usage: stat <file>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() != 1) {
            usage();
            return;
        }
        try {
            auto f = state.lookup(args[0]);
            auto attr = f->getattr();
            cout << "fileid: "<< attr->fileid() << endl
                 << "type:   " << formatType(attr->type()) << endl
                 << "mode:   " << formatMode(attr->mode()) << endl
                 << "nlink:  " << attr->nlink() << endl
                 << "uid:    " << attr->uid() << endl
                 << "gid:    " << attr->gid() << endl
                 << "size:   " << attr->size() << endl
                 << "used:   " << attr->used() << endl
                 << "mtime:  " << formatTime(attr->mtime()) << endl
                 << "atime:  " << formatTime(attr->atime()) << endl
                 << "ctime:  " << formatTime(attr->ctime()) << endl
                 << "btime:  " << formatTime(attr->birthtime()) << endl;
        }
        catch (system_error& e) {
            cout << args[0] << ": " << e.what() << endl;
        }
    }
};

static CommandReg<StatCommand> reg;

}
