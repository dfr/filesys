#pragma once

#include <map>
#include <string>
#include <vector>

namespace filesys {

class CommandState
{
public:
    CommandState(std::shared_ptr<File> dir);
    std::shared_ptr<File> cwd() const { return cwd_; }
    std::shared_ptr<File> lookup(const std::string& name);
    std::shared_ptr<File> open(const std::string& name, int flags, int mode);
    std::shared_ptr<File> mkdir(const std::string& name, int mode);
    void chdir(std::shared_ptr<File> dir);

private:
    std::shared_ptr<File> root_;
    std::shared_ptr<File> cwd_;
};

class Command
{
public:
    virtual ~Command() {}

    virtual const char* name() const = 0;
    virtual const char* help() const = 0;
    virtual void usage() = 0;
    virtual void run(CommandState& state, std::vector<std::string>& args) = 0;
};

class CommandSet
{
public:
    typedef std::map<std::string, std::shared_ptr<Command>> commandMapT;

    static CommandSet& instance()
    {
        static CommandSet set;
        return set;
    }

    void add(std::shared_ptr<Command> cmd)
    {
        commands_[cmd->name()] = cmd;
    }

    std::shared_ptr<Command> lookup(const std::string& name)
    {
        auto i = commands_.find(name);
        if (i == commands_.end())
            return nullptr;
        return i->second;
    }

    commandMapT::const_iterator begin() const { return commands_.begin(); }
    commandMapT::const_iterator end() const { return commands_.end(); }

private:
    commandMapT commands_;
};

template <typename CMD>
class CommandReg
{
public:
    CommandReg()
    {
        CommandSet::instance().add(std::make_shared<CMD>());
    }
};

}
