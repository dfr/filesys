/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#include <iostream>

#include "keyval/kvtool/kvtool.h"

using namespace std;

namespace keyval {

class QuitCommand: public Command
{
public:
    const char* name() const override { return "quit"; }

    const char* help() const override
    {
        return "exit";
    }

    void usage() override
    {
        cout << "usage: quit" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() != 0) {
            usage();
            return;
        }
	state.setQuit(true);
    }
};

static CommandReg<QuitCommand> reg;

}
