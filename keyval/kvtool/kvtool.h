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
