/*-
 * Copyright (c) 2016-present Doug Rabson
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
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
