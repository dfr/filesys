#include <iostream>

#include <fs++/filesys.h>

#include "cli/fscli.h"

using namespace std;

namespace filesys {

class LnCommand: public Command
{
public:
    const char* name() const override { return "ln"; }

    const char* help() const override
    {
        return "link a file";
    }

    void usage() override
    {
        cout << "usage: ln [-s] <from> <to>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        auto& cred = state.cred();

        bool symlink = false;
        if (args.size() > 0 && args[0] == "-s") {
            symlink = true;
            args.erase(args.begin());
        }
        if (args.size() != 2) {
            usage();
            return;
        }
        if (symlink) {
            try {
                auto f = state.symlink(args[1], args[0]);
            }
            catch (system_error& e) {
                cout << args[0] << ": " << e.what() << endl;
            }
        }
        else {
            pair<shared_ptr<File>, string> from, to;
            shared_ptr<File> fromFile;
            try {
                from = state.resolvepath(args[0]);
                fromFile = from.first->lookup(cred, from.second);
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

            // If the target exists, we assume its a directory and that
            // the desired name is the same as the source
            try {
                auto f = to.first->lookup(cred, to.second);
                to.first = f;
                to.second = leafEntry(args[0]);
            }
            catch (system_error& e) {
                // Ignore
            }

            try {
                if (to.first->fs() != fromFile->fs())
                    throw system_error(EXDEV, system_category());
                to.first->link(cred, to.second, fromFile);
            }
            catch (system_error& e) {
                cout << args[0] << ", " << args[1] << ": "<< e.what() << endl;
            }
        }
    }
};

static CommandReg<LnCommand> reg;

}
