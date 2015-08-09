#include <array>
#include <cctype>
#include <iostream>

#include <fs++/filesys.h>
#include <fs++/nfsfs.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "cli/command.h"

using namespace filesys;
using namespace std;

void executeCommand(CommandState& state, const string& line)
{
    vector<string> words;
    string word;
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
        cout << commandName << ": command not found" << endl;
}

int main(int argc, char** argv)
{
    gflags::SetUsageMessage("usage: fscli <url>");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 2) {
        gflags::ShowUsageWithFlagsRestrict(argv[0], "fscli.cpp");
        return 1;
    }

    google::InitGoogleLogging(argv[0]);

    NfsFilesystemFactory fac;
    auto fs = fac.mount(argv[1]);
    CommandState state(fs->root());

    while (!cin.eof()) {
        cout << "FSCLI> ";
        cout.flush();
        array<char, 512> line;
        cin.getline(line.data(), line.size());
        if (!line[0])
            break;
        executeCommand(state, line.data());
    }
}
