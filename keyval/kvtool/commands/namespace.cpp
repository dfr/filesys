/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <iostream>

#include <keyval/keyval.h>
#include "keyval/kvtool/kvtool.h"

using namespace std;

namespace keyval {

class NamespaceCommand: public Command
{
public:
    const char* name() const override { return "namespace"; }

    const char* help() const override
    {
        return "Change the key namespace";
    }

    void usage() override
    {
        cout << "usage: namespace <name>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() != 1) {
            usage();
            return;
        }
        state.setCurrentNamespace(args[0]);
    }
};

static CommandReg<NamespaceCommand> reg;

}
