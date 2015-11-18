#include <iostream>

#include <fs++/filesys.h>

#include "cli/fscli.h"

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
        auto& cred = state.cred();

        if (args.size() != 2) {
            usage();
            return;
        }
        pair<shared_ptr<File>, string> from, to;
        try {
            from = state.resolvepath(args[0]);
            from.first->lookup(cred, from.second);
        }
        catch (system_error& e) {
            cout << args[0] << ": " << e.what() << endl;
            return;
        }
        try {
            to = state.resolvepath(args[1]);
        }
        catch (system_error& e) {
            cout << args[1] << ": " << e.what() << endl;
            return;
        }

        // If the target exists, and it is a directory, we copy to a
        // destination in that directory with the same name as the source
        try {
            auto f = to.first->lookup(cred, to.second);
            if (f->getattr()->type() == FileType::DIRECTORY) {
                to.first = f;
                to.second = leafEntry(args[0]);
            }
        }
        catch (system_error& e) {
            // Ignore
        }

        shared_ptr<File> in, out;
        try {
            in = from.first->open(
                cred, from.second, OpenFlags::READ,
                [](Setattr* sattr){});
        }
        catch (system_error& e) {
            cout << args[0] << ": " << e.what() << endl;
            return;
        }
        try {
            out = to.first->open(
                cred, to.second,
                OpenFlags::WRITE | OpenFlags::CREATE | OpenFlags::TRUNCATE,
                [](Setattr* sattr){ sattr->setMode(0666); });
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
                auto data = in->read(cred, offset, 1024, eof);
                // XXX: handle short writes
                out->write(cred, offset, data);
                offset += data->size();
            }
            in->close(cred);
            out->commit(cred);
            out->close(cred);
        }
        catch (system_error& e) {
            cout << e.what() << endl;
        }
    }
};

static CommandReg<CpCommand> reg;

}
