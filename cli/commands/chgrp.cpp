/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#include <iostream>

#include <filesys/filesys.h>

#include "cli/fscli.h"

using namespace std;

namespace filesys {

class ChgrpCommand: public Command
{
public:
    const char* name() const override { return "chgrp"; }

    const char* help() const override
    {
        return "change file group";
    }

    void usage() override
    {
        cout << "usage: chgrp <gid> <file>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        auto& cred = state.cred();

        if (args.size() != 2) {
            usage();
            return;
        }
        size_t sz;
        auto gid = stoi(args[0], &sz, 10);
        if (sz != args[0].size()) {
            usage();
            return;
        }
        try {
            auto f = state.lookup(args[1]);
            f->setattr(cred, [gid](Setattr* attr){ attr->setGid(gid); });
        }
        catch (system_error& e) {
            cout << args[0] << ": " << e.what() << endl;
        }
    }
};

static CommandReg<ChgrpCommand> reg;

}
