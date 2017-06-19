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


#include "pfsfs.h"

using namespace filesys;
using namespace filesys::pfs;
using namespace std;

PfsDirectoryIterator::PfsDirectoryIterator(
    const std::map<std::string, std::weak_ptr<PfsFile>>& entries)
    : entries_(entries),
      p_(entries_.begin())
{
    skipExpired();
}

bool PfsDirectoryIterator::valid() const
{
    return p_ != entries_.end();
}

FileId PfsDirectoryIterator::fileid() const
{
    return p_->second.lock()->fileid();
}

uint64_t PfsDirectoryIterator::seek() const
{
    return 0;
}

std::string PfsDirectoryIterator::name() const
{
    return p_->first;
}

std::shared_ptr<File> PfsDirectoryIterator::file() const
{
    return p_->second.lock()->checkMount();
}

void PfsDirectoryIterator::next()
{
    ++p_;
    skipExpired();
}

void PfsDirectoryIterator::skipExpired()
{
    while (p_ != entries_.end() && p_->second.expired())
        ++p_;
}
