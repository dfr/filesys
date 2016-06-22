/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <array>
#include <cctype>
#include <fstream>
#include <iostream>

#include <fs++/filesys.h>
#include <fs++/urlparser.h>

#include "filesys/pfs/pfsfs.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "cli/fscli.h"

using namespace filesys;
using namespace std;

DEFINE_string(c, "", "file of commands to execute");
DEFINE_string(realm, "", "Local krb5 realm name");

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

    gflags::SetUsageMessage("usage: fscli [path+]<url> ...");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc < 2) {
        gflags::ShowUsageWithFlagsRestrict(argv[0], "fscli.cpp");
        return 1;
    }

    google::InitGoogleLogging(argv[0]);

    auto& fsman = FilesystemManager::instance();

    auto pfs = make_shared<filesys::pfs::PfsFilesystem>();

    for (int i = 1; i < argc; i++) {
	string url = argv[i];
	string path = "/";

	UrlParser p(url);
        auto it = p.query.find("path");
        if (it != p.query.end()) {
            path = it->second;
        }

        auto fac = fsman.find(p.scheme);
        if (!fac) {
            cerr << url << ": unsupported url scheme" << endl;
            return 1;
        }

        auto fs = fac->mount(url);
        fsman.mount(url, fs);

        auto dir = fs->root();
        if (p.isHostbased() && p.path.size() > 0) {
            dir = CommandState(fs->root()).lookup(p.path);
        }
        pfs->add(path, dir);
    }

    CommandState state(pfs->root());

    istream* input;
    if (FLAGS_c.size() > 0) {
        input = new ifstream(FLAGS_c);
        interactive = false;
    }
    else {
        input = &cin;
    }

    while (!input->eof() && !state.quit()) {
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

    fsman.unmountAll();
}
