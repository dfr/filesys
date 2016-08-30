/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#pragma once

#include <map>
#include <string>
#include <vector>

namespace keyval {

class Database;

class CommandState
{
public:
    CommandState(std::shared_ptr<Database> db)
        : db_(db),
          currentNamespace_("default")
    {
    }

    auto db() const { return db_; }
    auto& currentNamespace() const { return currentNamespace_; }
    bool quit() const { return quit_; }
    void setQuit(bool v) { quit_ = v; }

    void setCurrentNamespace(const std::string& ns)
    {
        currentNamespace_ = ns;
    }

private:
    std::shared_ptr<Database> db_;
    std::string currentNamespace_;
    bool quit_ = false;
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
