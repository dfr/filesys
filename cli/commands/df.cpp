/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
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
