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

#include <filesys/filesys.h>

#include "cli/fscli.h"

using namespace std;

namespace filesys {

class DfCommand: public Command
{
public:
    const char* name() const override { return "df"; }

    const char* help() const override
    {
        return "filesystem attributes";
    }

    void usage() override
    {
        cout << "usage: df [<directory>]" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() > 1) {
            usage();
            return;
        }
        TableFormatter<40, 8, 8, 8> tf(cout);
        tf("Filesystem", "Size", "Used", "Avail");
        vector<pair<string, shared_ptr<File>>> mntlist;
        try {
            if (args.size() == 1) {
                auto dir = state.lookup(args[0]);
                mntlist.push_back(make_pair(args[0], dir));
            }
            else {
                for (auto mnt: FilesystemManager::instance())
                    mntlist.push_back(
                        make_pair(mnt.first, mnt.second->root()));
            }
        }
        catch (system_error& e) {
            cout << args[0] << ": " << e.what() << endl;
        }
        for (auto& mnt: mntlist) {
            auto stat = mnt.second->fsstat(state.cred());
            tf(mnt.first,
               humanizeNumber(stat->totalSpace()),
               humanizeNumber(stat->totalSpace() - stat->freeSpace()),
               humanizeNumber(stat->availSpace()));
        }
    }
};

static CommandReg<DfCommand> reg;

}
