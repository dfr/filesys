#include <iostream>

#include <fs++/filesys.h>

#include "cli/command.h"

using namespace std;

namespace filesys {

class MkdirCommand: public Command
{
public:
    const char* name() const override { return "mkdir"; }

    const char* help() const override
    {
        return "create a new directory";
    }

    void usage() override
    {
        cout << "usage: mkdir <directory>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() != 1) {
            usage();
            return;
        }
        try {
            auto f = state.mkdir(args[0], 0777);
        }
        catch (system_error& e) {
            cout << args[0] << ": " << e.what() << endl;
        }
    }
};

static CommandReg<MkdirCommand> reg;

}
