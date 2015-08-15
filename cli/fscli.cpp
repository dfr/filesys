#include <array>
#include <cctype>
#include <fstream>
#include <iostream>

#include <fs++/filesys.h>
#include <fs++/urlparser.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "cli/fscli.h"

using namespace filesys;
using namespace std;

DEFINE_string(c, "", "file of commands to execute");

deque<string> filesys::parsePath(const string& path)
{
    deque<string> res;
    string entry;
    for (auto ch: path) {
        if (ch == '/') {
            if (entry.size() > 0) {
                res.push_back(entry);
                entry.clear();
            }
        }
        else {
            entry.push_back(ch);
        }
    }
    if (entry.size() > 0)
        res.push_back(entry);
    return res;
}

std::string filesys::leafEntry(const std::string& path)
{
    auto tmp = parsePath(path);
    if (tmp.size())
        return tmp.back();
    else
        return ".";
}

string filesys::humanizeNumber(long val)
{
    static char suffix[] = "KMGTPE";
    int i;

    if (val < 1000l)
        return to_string(val);

    double v = double(val / 1000);
    i = 0;
    while (v > 1000.0 && i < 6) {
        v /= 1000.0;
        i++;
    }
    char buf[10];
    if (v < 10)
        snprintf(buf, sizeof(buf), "%.1f", v);
    else
        snprintf(buf, sizeof(buf), "%ld", long(v));
    return string(buf) + suffix[i];
}

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
    bool interactive = ::isatty(0);

    gflags::SetUsageMessage("usage: fscli <url>");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 2) {
        gflags::ShowUsageWithFlagsRestrict(argv[0], "fscli.cpp");
        return 1;
    }

    google::InitGoogleLogging(argv[0]);

    auto& fsman = FilesystemManager::instance();

    UrlParser p(argv[1]);
    auto fac = fsman.find(p.scheme);
    if (!fac) {
        cerr << argv[1] << ": unsupported url scheme" << endl;
        return 1;
    }

    auto mnt = fac->mount(argv[1]);
    CommandState state(mnt.first->root());
    state.chdir(state.lookup(mnt.second));

    istream* input;
    if (FLAGS_c.size() > 0) {
        input = new ifstream(FLAGS_c);
        interactive = false;
    }
    else {
        input = &cin;
    }

    while (!input->eof()) {
        if (interactive) {
            cout << "FSCLI> ";
            cout.flush();
        }
        array<char, 512> line;
        input->getline(line.data(), line.size());
        if (!line[0])
            continue;
        executeCommand(state, line.data());
    }
    if (input != &cin)
        delete input;
}
