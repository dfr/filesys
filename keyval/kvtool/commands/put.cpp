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

#include <keyval/keyval.h>
#include "keyval/kvtool/kvtool.h"

using namespace std;

namespace keyval {

static std::shared_ptr<Buffer> toBuffer(const std::string& s)
{
    return std::make_shared<keyval::Buffer>(s);
}

class PutCommand: public Command
{
public:
    const char* name() const override { return "put"; }

    const char* help() const override
    {
        return "Set the value for a key";
    }

    void usage() override
    {
        cout << "usage: put <key> <value>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() != 2) {
            usage();
            return;
        }
        auto ns = state.db()->getNamespace(state.currentNamespace());
        auto trans = state.db()->beginTransaction();
        trans->put(ns, toBuffer(args[0]), toBuffer(args[1]));
        state.db()->commit(std::move(trans));
    }
};

static CommandReg<PutCommand> reg;

}
