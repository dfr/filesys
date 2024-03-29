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

#include <iomanip>
#include <iostream>

#include "keyval/kvtool/kvtool.h"

using namespace std;

namespace keyval {

class HelpCommand: public Command
{
public:
    const char* name() const override { return "help"; }

    const char* help() const override
    {
        return "show command help";
    }

    void usage() override
    {
        cout << "usage: help [<command>]" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() > 1) {
            usage();
            return;
        }
        if (args.size() == 0) {
            for (auto& entry: CommandSet::instance()) {
                cout << left << setw(8) << entry.first
                     << setw(0) << " - " << entry.second->help() << endl;
            }
        }
        else {
            auto cmd = CommandSet::instance().lookup(args[0]);
            cmd->usage();
        }
    }
};

static CommandReg<HelpCommand> reg;

}
