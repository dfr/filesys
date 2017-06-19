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

#include <iostream>
#include <sstream>

#include <filesys/filesys.h>

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
