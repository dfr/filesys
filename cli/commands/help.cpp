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

class HelpCommand: public Command
{
public:
    const char* name() const override { return "help"; }

    const char* help() const override
    {
        return "show command help";
    }

    void usage() override
    {
        cout << "usage: help [<command>]" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() > 1) {
            usage();
            return;
        }
        if (args.size() == 0) {
            for (auto& entry: CommandSet::instance()) {
                cout << left << setw(8) << entry.first
                     << setw(0) << " - " << entry.second->help() << endl;
            }
        }
        else {
            auto cmd = CommandSet::instance().lookup(args[0]);
            cmd->usage();
        }
    }
};

static CommandReg<HelpCommand> reg;

}
