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

#include <array>
#include <cctype>
#include <fstream>
#include <iostream>
#include <thread>

#include <keyval/keyval.h>
#include <rpc++/socket.h>
#include <rpc++/urlparser.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kvtool.h"

using namespace keyval;

DEFINE_string(c, "", "File of commands to execute");
DEFINE_string(replica, "", "Network address to contact replicas");

void executeCommand(CommandState& state, const std::string& line)
{
    std::vector<std::string> words;
    std::string word;
    for (auto ch: line) {
        if (isspace(ch)) {
            if (word.size() > 0) {
                words.push_back(move(word));
            }
        }
        else {
            word += ch;
        }
    }
    if (word.size() > 0)
        words.push_back(move(word));
    if (words.size() == 0)
        return;
    auto commandName = words[0];
    words.erase(words.begin());
    auto command = CommandSet::instance().lookup(commandName);
    if (command)
        command->run(state, words);
    else
        std::cout << commandName << ": command not found" << std::endl;
}

int main(int argc, char** argv)
{
    bool interactive = ::isatty(0);

    gflags::SetUsageMessage("usage: kvtool <path>");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 2) {
        gflags::ShowUsageWithFlagsRestrict(argv[0], "kvtool.cpp");
        return 1;
    }

    google::InitGoogleLogging(argv[0]);

    auto sockman = std::make_shared<oncrpc::SocketManager>();
    std::shared_ptr<Database> db;
    if (FLAGS_replica.size() > 0) {
        db = make_paxosdb(argv[1], FLAGS_replica, sockman);
    }
    else {
        db = make_rocksdb(argv[1]);
    }
    CommandState state(db);

    std::istream* input;
    if (FLAGS_c.size() > 0) {
        input = new std::ifstream(FLAGS_c);
        interactive = false;
    }
    else {
        input = &std::cin;
    }

    std::thread t([sockman]() { sockman->run(); });

    while (!input->eof() && !state.quit()) {
        if (interactive) {
            std::cout << state.currentNamespace() << "> ";
            std::cout.flush();
        }
        std::array<char, 512> line;
        input->getline(line.data(), line.size());
        if (!line[0])
            continue;
        executeCommand(state, line.data());
    }

    sockman->stop();
    t.join();

    if (input != &std::cin)
        delete input;
}
