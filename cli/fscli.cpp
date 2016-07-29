/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <array>
#include <cctype>
#include <fstream>
#include <iostream>

#include <filesys/filesys.h>
#include <rpc++/urlparser.h>

#include "filesys/pfs/pfsfs.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "cli/fscli.h"

using namespace filesys;
using namespace std;

DEFINE_string(c, "", "file of commands to execute");
DEFINE_string(realm, "", "Local krb5 realm name");
DEFINE_string(fsid, "", "Override file system identifier for new filesystems");

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
    double divisor = 1024.0;
    int i;

    if (val < divisor)
        return to_string(val);

    double v = double(val) / divisor;
    i = 0;
    while (v > divisor && i < 6) {
        v /= divisor;
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

        oncrpc::UrlParser p(url);
        auto it = p.query.find("path");
        if (it != p.query.end()) {
            path = it->second;
        }

        auto fac = fsman.find(p.scheme);
        if (!fac) {
            cerr << url << ": unsupported url scheme" << endl;
            return 1;
        }

        shared_ptr<Filesystem> fs;
        try {
            fs = fac->mount(url);
        }
        catch (runtime_error& e) {
            cerr << url << ": " << e.what() << endl;
            return 1;
        }
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
}
