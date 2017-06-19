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

#include <cctype>
#include <system_error>

#include <filesys/filesys.h>

#include "cli/fscli.h"

using namespace filesys;
using namespace std;

CommandState::CommandState(shared_ptr<File> root)
    : cred_(0, 0, {}, true),
      root_(root),
      cwd_(root)
{
}

static int parseHexDigit(char ch)
{
    if (ch >= '0' && ch <= '9')
        return ch - '0';
    if (ch >= 'a' && ch <= 'f')
        return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F')
        return ch - 'A' + 10;
    throw system_error(ENOENT, system_category());
}

static vector<uint8_t> parseByteArray(const string& s)
{
    vector<uint8_t> res;
    uint8_t b;

    if (s.size() & 1)
        throw system_error(ENOENT, system_category());
    for (auto i = 0; i < int(s.size()); i++) {
        auto n = parseHexDigit(s[i]);
        if (i & 1) {
            b |= n;
            res.push_back(b);
        }
        else {
            b = n << 4;
        }
    }
    return res;
}

static shared_ptr<File> lookupfh(const string& s)
{
    auto handle = parseByteArray(s.substr(3));
    FileHandle fh{1, handle};
    return FilesystemManager::instance().find(fh);
}

shared_ptr<File> CommandState::lookup(const string& name)
{
    if (name.substr(0, 3) == "FH:") {
        return lookupfh(name);
    }
    auto p = resolvepath(name);
    return p.first->lookup(cred_, p.second);
}

shared_ptr<OpenFile> CommandState::open(
    const string& name, int flags, int mode)
{
    auto p = resolvepath(name);
    return p.first->open(
        cred_, p.second, flags,
        [mode](Setattr* sattr){ sattr->setMode(mode); });
}

shared_ptr<File> CommandState::mkdir(const string& name, int mode)
{
    auto p = resolvepath(name);
    return p.first->mkdir(
        cred_, p.second, [mode](Setattr* sattr){ sattr->setMode(mode); });
}

shared_ptr<File> CommandState::symlink(const string& name, const string& path)
{
    auto p = resolvepath(name);
    return p.first->symlink(
        cred_, p.second, path, [](Setattr* sattr){ sattr->setMode(0777); });
}

std::shared_ptr<File> CommandState::mkfifo(const std::string& name)
{
    auto p = resolvepath(name);
    return p.first->mkfifo(
        cred_, p.second, [](Setattr* sattr){ sattr->setMode(0666); });
}

void CommandState::remove(const std::string& name)
{
    auto p = resolvepath(name, false);
    p.first->remove(cred_, p.second);
}

void CommandState::rmdir(const std::string& name)
{
    auto p = resolvepath(name);
    p.first->rmdir(cred_, p.second);
}

void CommandState::chdir(shared_ptr<File> dir)
{
    cwd_ = dir;
}

std::pair<std::shared_ptr<File>, std::string>
CommandState::resolvepath(const std::string& name, bool follow)
{
    shared_ptr<File> f;
    auto path = parsePath(name);
    if (name[0] == '/') {
        f = root_;
    }
    else {
        f = cwd_;
    }
restart:
    if (path.size() == 0) {
        return make_pair(f, ".");
    }
    auto leafEntry = path.back();
    path.pop_back();
    while (path.size() > 0) {
        auto entry = path.front();
        path.pop_front();
        f = f->lookup(cred_, entry);
        if (f->getattr()->type() == FileType::SYMLINK) {
            auto dest = f->readlink(cred_);
            auto newpath = parsePath(dest);
            if (dest[0] == '/')
                f = root_;
            for (auto& entry: path)
                newpath.push_back(entry);
            path = newpath;
        }
    }
    try {
        // If the leaf exists, check for symbolic links
        auto leaf = f->lookup(cred_, leafEntry);
        if (follow && leaf->getattr()->type() == FileType::SYMLINK) {
            auto dest = leaf->readlink(cred_);
            if (dest[0] == '/')
                f = root_;
            path = parsePath(dest);
            goto restart;
        }
    }
    catch (system_error& e) {
        // Ignore
    }
    return make_pair(f, leafEntry);
}
