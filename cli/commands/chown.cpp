/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <iostream>

#include <fs++/filesys.h>

#include "cli/fscli.h"

using namespace std;

namespace filesys {

class ChownCommand: public Command
{
public:
    const char* name() const override { return "chown"; }

    const char* help() const override
    {
        return "change file owner";
    }

    void usage() override
    {
        cout << "usage: chown <uid> <file>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        auto& cred = state.cred();

        if (args.size() != 2) {
            usage();
            return;
        }
        size_t sz;
        auto uid = stoi(args[0], &sz, 10);
        if (sz != args[0].size()) {
            usage();
            return;
        }
        try {
            auto f = state.lookup(args[1]);
            f->setattr(cred, [uid](Setattr* attr){ attr->setUid(uid); });
        }
        catch (system_error& e) {
            cout << args[0] << ": " << e.what() << endl;
        }
    }
};

static CommandReg<ChownCommand> reg;

}
