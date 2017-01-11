/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#include <iostream>

#include <keyval/keyval.h>
#include "keyval/kvtool/kvtool.h"

using namespace std;

namespace keyval {

static std::shared_ptr<Buffer> toBuffer(const std::string& s)
{
    return std::make_shared<keyval::Buffer>(s);
}

class PutCommand: public Command
{
public:
    const char* name() const override { return "put"; }

    const char* help() const override
    {
        return "Set the value for a key";
    }

    void usage() override
    {
        cout << "usage: put <key> <value>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() != 2) {
            usage();
            return;
        }
        auto ns = state.db()->getNamespace(state.currentNamespace());
        auto trans = state.db()->beginTransaction();
        trans->put(ns, toBuffer(args[0]), toBuffer(args[1]));
        state.db()->commit(std::move(trans));
    }
};

static CommandReg<PutCommand> reg;

}
