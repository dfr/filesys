#include <iostream>

#include <fs++/filesys.h>

#include "cli/command.h"

using namespace std;

namespace filesys {

class CpCommand: public Command
{
public:
    const char* name() const override { return "cp"; }

    const char* help() const override
    {
        return "copy a file";
    }

    void usage() override
    {
        cout << "usage: cp <from> <to>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() != 2) {
            usage();
            return;
        }

        shared_ptr<File> in, out;
        try {
            in = state.open(args[0], OpenFlags::READ, 0);
        }
        catch (system_error& e) {
            cout << args[0] << ": " << e.what() << endl;
            return;
        }
        try {
            out = state.open(
                args[1],
                OpenFlags::WRITE | OpenFlags::CREATE | OpenFlags::TRUNCATE,
                0666);
        }
        catch (system_error& e) {
            cout << args[1] << ": " << e.what() << endl;
            return;
        }

        try {
            if (in->getattr()->type() != FileType::FILE) {
                error_code ec(EISDIR, system_category());
                cout << args[0] << ": " << ec.message() << endl;
                return;
            }
            if (out->getattr()->type() != FileType::FILE) {
                error_code ec(EISDIR, system_category());
                cout << args[1] << ": " << ec.message() << endl;
                return;
            }
            uint64_t offset = 0;
            bool eof = false;
            while (!eof) {
                // XXX: fix rpc buffer sizes
                auto data = in->read(offset, 1024, eof);
                // XXX: handle short writes
                out->write(offset, data);
                offset += data.size();
            }
            in->close();
            out->commit();
            out->close();
        }
        catch (system_error& e) {
            cout << e.what() << endl;
        }
    }
};

static CommandReg<CpCommand> reg;

}
