#include <iostream>

#include <fs++/filesys.h>

#include "cli/fscli.h"

using namespace std;

namespace filesys {

class CdCommand: public Command
{
public:
    const char* name() const override { return "cd"; }

    const char* help() const override
    {
        return "change directory";
    }

    void usage() override
    {
        cout << "usage: cd <directory>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() != 1) {
            usage();
            return;
        }
        try {
            auto dir = state.lookup(args[0]);
            if (dir->getattr()->type() != FileType::DIRECTORY)
                throw system_error(ENOTDIR, system_category());
            state.chdir(dir);
        }
        catch (system_error& e) {
            cout << args[0] << ": " << e.what() << endl;
        }
    }
};

static CommandReg<CdCommand> reg;

}
