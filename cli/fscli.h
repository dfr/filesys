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

#include <deque>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <vector>

namespace filesys {

std::deque<std::string> parsePath(const std::string& path);
std::string leafEntry(const std::string& path);
std::string humanizeNumber(long val);

template <int... Widths>
class TableFormatter
{
public:
    TableFormatter(std::ostream& str) : str_(str)
    {
    }

    template <typename... T>
    void operator()(T... fields)
    {
        format(std::make_pair(Widths, fields)...);
        str_ << std::endl;
    }

private:
    template <typename T>
    void format(const std::pair<int, T>& field)
    {
        if (field.first < 0)
            str_ << std::right << std::setw(-field.first) << field.second;
        else
            str_ << std::left << std::setw(field.first) << field.second;
    }

    template <typename T, typename... Rest>
    void format(const std::pair<int, T>& field, Rest... rest)
    {
        format(field);
        format(rest...);
    }

    std::ostream& str_;
};

class CommandState
{
public:
    CommandState(std::shared_ptr<File> dir);
    std::shared_ptr<File> cwd() const { return cwd_; }
    std::shared_ptr<File> lookup(const std::string& name);
    std::shared_ptr<OpenFile> open(
	const std::string& name, int flags, int mode);
    std::shared_ptr<File> mkdir(const std::string& name, int mode);
    std::shared_ptr<File> symlink(
        const std::string& name, const std::string& path);
    std::shared_ptr<File> mkfifo(const std::string& name);
    void remove(const std::string& path);
    void rmdir(const std::string& path);
    void chdir(std::shared_ptr<File> dir);
    std::pair<std::shared_ptr<File>, std::string> resolvepath(
        const std::string& path, bool follow = true);

    auto& cred() const { return cred_; }
    bool quit() const { return quit_; }
    void setQuit(bool v) { quit_ = v; }

private:
    Credential cred_;
    std::shared_ptr<File> root_;
    std::shared_ptr<File> cwd_;
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
