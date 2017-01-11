/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#include <iostream>

#include <filesys/filesys.h>

#include "cli/fscli.h"

using namespace std;

namespace filesys {

class RmdirCommand: public Command
{
public:
    const char* name() const override { return "rmdir"; }

    const char* help() const override
    {
        return "remove a directory";
    }

    void usage() override
    {
        cout << "usage: rmdir <directory>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() != 1) {
            usage();
            return;
        }
        try {
            state.rmdir(args[0]);
        }
        catch (system_error& e) {
            cout << args[0] << ": " << e.what() << endl;
        }
    }
};

static CommandReg<RmdirCommand> reg;

}
