#include <algorithm>
#include <sstream>

#include <fs++/filesys.h>

#include "cli/fscli.h"

using namespace std;

namespace filesys {

static void printByteArray(const vector<uint8_t>& buf)
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
        if (args.size() > 1) {
            usage();
            return;
        }
        try {
            typedef pair<string, shared_ptr<File>> entryT;
            vector<entryT> files;
            auto dir = args.size() == 0 ? state.cwd() : state.lookup(args[0]);
            if (dir->getattr()->type() == FileType::DIRECTORY) {
                for (auto iter = dir->readdir(); iter->valid(); iter->next()) {
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
                FileHandle fh;
                f->handle(fh);
                cout << setw(15) << left << name << " FH:";
                printByteArray(fh.fsid);
                cout << "/";
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
