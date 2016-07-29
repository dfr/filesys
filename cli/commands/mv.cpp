/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <iostream>

#include <filesys/filesys.h>

#include "cli/fscli.h"

using namespace std;

namespace filesys {

class MvCommand: public Command
{
public:
    const char* name() const override { return "mv"; }

    const char* help() const override
    {
        return "move a file or directory";
    }

    void usage() override
    {
        cout << "usage: mv <from> <to>" << endl;
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

        // If the target exists, and its a directory set the target to that
        // directory with the same name as the source
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

        try {
            if (to.first->fs() != from.first->fs())
                throw system_error(EXDEV, system_category());
            to.first->rename(cred, to.second, from.first, from.second);
        }
        catch (system_error& e) {
            cout << args[0] << ", " << args[1] << ": "<< e.what() << endl;
        }
    }
};

static CommandReg<MvCommand> reg;

}
