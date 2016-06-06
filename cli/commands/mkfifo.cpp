/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <iostream>

#include <fs++/filesys.h>

#include "cli/fscli.h"

using namespace std;

namespace filesys {

class MkfifoCommand: public Command
{
public:
    const char* name() const override { return "mkfifo"; }

    const char* help() const override
    {
        return "create a named pipe";
    }

    void usage() override
    {
        cout << "usage: mkfifo <name>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() != 1) {
            usage();
            return;
        }
        try {
            auto f = state.mkfifo(args[0]);
        }
        catch (system_error& e) {
            cout << args[0] << ": " << e.what() << endl;
        }
    }
};

static CommandReg<MkfifoCommand> reg;

}
