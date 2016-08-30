/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
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
