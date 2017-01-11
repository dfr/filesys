/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#include <iomanip>
#include <iostream>

#include <filesys/filesys.h>

#include "cli/fscli.h"

using namespace std;

namespace filesys {

class FsidCommand: public Command
{
public:
    const char* name() const override { return "fsid"; }

    const char* help() const override
    {
        return "exit";
    }

    void usage() override
    {
        cout << "usage: fsid" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() != 0) {
            usage();
            return;
        }
	auto fsid = state.cwd()->fs()->fsid();

        auto savefill = cout.fill();
        auto saveflags = cout.flags();
        cout << std::hex << std::setfill('0');
        for (auto v: fsid)
            cout << std::setw(2) << int(v);
        cout << endl;
        cout.fill(savefill);
        cout.flags(saveflags);
    }
};

static CommandReg<FsidCommand> reg;

}
