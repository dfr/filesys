#include <iostream>

#include <fs++/filesys.h>

#include "cli/fscli.h"

using namespace std;

namespace filesys {

class CatCommand: public Command
{
public:
    const char* name() const override { return "cat"; }

    const char* help() const override
    {
        return "show the contents of a file";
    }

    void usage() override
    {
        cout << "usage: cat <file>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() != 1) {
            usage();
            return;
        }
        try {
            auto of = state.open(args[0], OpenFlags::READ, 0666);
            if (of->file()->getattr()->type() != FileType::FILE)
                throw system_error(EISDIR, system_category());
            uint64_t offset = 0;
            bool eof = false;
            while (!eof) {
                auto data = of->read(offset, 8192, eof);
                cout.write(reinterpret_cast<char*>(data->data()), data->size());
                offset += data->size();
            }
        }
        catch (system_error& e) {
            cout << args[0] << ": " << e.what() << endl;
        }
    }
};

static CommandReg<CatCommand> reg;

}
