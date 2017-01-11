/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#include <iostream>

#include <filesys/filesys.h>

#include "cli/fscli.h"

using namespace std;

namespace filesys {

class RmCommand: public Command
{
public:
    const char* name() const override { return "rm"; }

    const char* help() const override
    {
        return "remove a file";
    }

    void usage() override
    {
        cout << "usage: rm <file>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() != 1) {
            usage();
            return;
        }
        try {
            state.remove(args[0]);
        }
        catch (system_error& e) {
            cout << args[0] << ": " << e.what() << endl;
        }
    }
};

static CommandReg<RmCommand> reg;

}
