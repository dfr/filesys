/*-
 * Copyright (c) 2016 Doug Rabson
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

static std::string toString(std::shared_ptr<keyval::Buffer> buf)
{
    return std::string(
        reinterpret_cast<const char*>(buf->data()), buf->size());
}

class GetCommand: public Command
{
public:
    const char* name() const override { return "get"; }

    const char* help() const override
    {
        return "Get the value for a key";
    }

    void usage() override
    {
        cout << "usage: get <key>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() != 1) {
            usage();
            return;
        }
        auto ns = state.db()->getNamespace(state.currentNamespace());
        try {
            auto val = ns->get(toBuffer(args[0]));
            cout << "Value: " << toString(val) << endl;
        }
        catch (std::system_error& e) {
            cout << e.what() << endl;
        }
    }
};

static CommandReg<GetCommand> reg;

}
