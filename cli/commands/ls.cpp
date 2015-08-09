#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>

#include <fs++/filesys.h>

#include "cli/command.h"

using namespace std;

namespace {

template <int... Widths>
class TableFormatter
{
public:
    TableFormatter(ostream& str) : str_(str)
    {
    }

    template <typename... T>
    void operator()(T... fields)
    {
        format(make_pair(Widths, fields)...);
        str_ << endl;
    }

private:
    template <typename T>
    void format(const pair<int, T>& field)
    {
        if (field.first < 0)
            str_ << right << setw(-field.first) << field.second;
        else
            str_ << left << setw(field.first) << field.second;
    }

    template <typename T, typename... Rest>
    void format(const pair<int, T>& field, Rest... rest)
    {
        format(field);
        format(rest...);
    }

    ostream& str_;
};

}

namespace filesys {

static string formatType(FileType type)
{
    switch (type) {
    case FileType::FILE:
        return "-";
    case FileType::DIRECTORY:
        return "d";
    case FileType::BLOCKDEV:
        return "b";
    case FileType::CHARDEV:
        return "c";
    case FileType::SYMLINK:
        return "l";
    case FileType::SOCKET:
        return "s";
    case FileType::BAD:
        return "?";
    case FileType::FIFO:
        return "f";
    }
}

static string formatMode(uint32_t mode)
{
    static string modes[] = {
        "---", "--x", "-w-", "-wx", "r--", "r-x", "rw-", "rwx"
    };
    return modes[(mode >> 6) & 7] + modes[(mode >> 3) & 7] + modes[mode & 7];
}

static string formatTime(chrono::system_clock::time_point time)
{
    ostringstream ss;
    time_t t = chrono::system_clock::to_time_t(time);
    auto tm = localtime(&t);
    ss << put_time(tm, "%Y-%m-%d %H:%M");
    return ss.str();
}

class LsCommand: public Command
{
public:
    const char* name() const override { return "ls"; }
    const char* help() const override
    {
        return "show the contents of a directory";
    }


    void usage() override
    {
        cout << "usage: ls <directory>" << endl;
    }

    void run(CommandState& state, vector<string>& args) override
    {
        if (args.size() > 1) {
            usage();
            return;
        }
        TableFormatter<11, 4, 5, 5, -7, 1, 17, 1> tf(cout);
        try {
            typedef pair<string, shared_ptr<File>> entryT;
            vector<entryT> files;
            auto dir = args.size() == 0 ? state.cwd() : state.lookup(args[0]);
            for (auto iter = dir->readdir(); iter->valid(); iter->next()) {
                auto name = iter->name();
                if (name == "." || name == "..")
                    continue;
                files.push_back(make_pair(name, iter->file()));
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
                auto attr = f->getattr();
                if (attr->type() == FileType::SYMLINK) {
                    name += " -> ";
                    name += f->readlink();
                }
                tf(formatType(attr->type()) + formatMode(attr->mode()),
                   attr->nlink(),
                   attr->uid(),
                   attr->gid(),
                   attr->size(),
                   " ",
                   formatTime(attr->mtime()),
                   name);
            }
        }
        catch (system_error& e) {
            cout << e.what() << endl;
        }
    }
};

static CommandReg<LsCommand> reg;

}
