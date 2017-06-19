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

#include <algorithm>
#include <sstream>

#include <filesys/filesys.h>

#include "cli/fscli.h"

using namespace std;

namespace filesys {

static void printByteArray(const oncrpc::bounded_vector<uint8_t, 128>& buf)
{
    static char hexdigits[] = "0123456789abcdef";
    for (auto byte: buf) {
        cout << hexdigits[(byte >> 4) & 15] << hexdigits[byte & 15];
    }
}

class LhCommand: public Command
{
public:
    const char* name() const override { return "lh"; }
    const char* help() const override
    {
        return "show the file handles of the files in a directory";
    }


    void usage() override
    {
        cout << "usage: lh <directory>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        auto& cred = state.cred();

        if (args.size() > 1) {
            usage();
            return;
        }
        try {
            typedef pair<string, shared_ptr<File>> entryT;
            vector<entryT> files;
            auto dir = args.size() == 0 ? state.cwd() : state.lookup(args[0]);
            if (dir->getattr()->type() == FileType::DIRECTORY) {
                for (auto iter = dir->readdir(cred, 0);
                    iter->valid(); iter->next()) {
                    files.push_back(make_pair(iter->name(), iter->file()));
                }
            }
            else {
                files.push_back(make_pair(args[0], dir));
            }
            struct cmp {
                int operator()(const entryT& a, const entryT& b) {
                    return a.first < b.first;
                }
            };
            sort(files.begin(), files.end(), cmp());
            for (auto& entry: files) {
                auto name = entry.first;
                auto f = entry.second;
                FileHandle fh = f->handle();
                cout << setw(15) << left << name << " FH:";
                printByteArray(fh.handle);
                cout << endl;
            }
        }
        catch (system_error& e) {
            cout << args[0] << ": " << e.what() << endl;
        }
    }
};

static CommandReg<LhCommand> reg;

}
