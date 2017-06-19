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

// -*- c++ -*-
#pragma once

#include <filesys/filesys.h>

namespace filesys {
namespace objfs {

/// Key type for our DB - indexing by 64bit integer id. For file
/// metadata, we use the fileid as index with fileid zero referencing
/// filesystem metadata. The id is encoded big endian to group
/// consecutive entries
struct KeyType {
    KeyType(std::uint64_t id)
        : buf_(std::make_shared<oncrpc::Buffer>(sizeof(std::uint64_t)))
    {
        std::uint64_t n = id;
        for (size_t i = 0; i < sizeof(n); i++) {
            buf_->data()[i] = (n >> 56) & 0xff;
            n <<= 8;
        }
    }

    KeyType(std::shared_ptr<oncrpc::Buffer> buf)
        : buf_(buf)
    {
        assert(buf->size() == sizeof(std::uint64_t));
    }

    operator std::shared_ptr<oncrpc::Buffer>() const
    {
        return buf_;
    }

    std::uint64_t id() const
    {
        std::uint64_t n = 0;
        for (size_t i = 0; i < sizeof(n); i++) {
            n = (n << 8) + buf_->data()[i];
        }
        return n;
    }

private:
    std::shared_ptr<oncrpc::Buffer> buf_;
};


/// Key type containing two 64bit fields
struct DoubleKeyType {
    DoubleKeyType(std::uint64_t id0, std::uint64_t id1)
        : buf_(std::make_shared<oncrpc::Buffer>(2*sizeof(std::uint64_t)))
    {
        std::uint64_t n = id0;
        for (size_t i = 0; i < sizeof(n); i++) {
            buf_->data()[i] = (n >> 56) & 0xff;
            n <<= 8;
        }
        n = id1;
        for (size_t i = 0; i < sizeof(n); i++) {
            buf_->data()[8+i] = (n >> 56) & 0xff;
            n <<= 8;
        }
    }

    DoubleKeyType(std::shared_ptr<oncrpc::Buffer> buf)
        : buf_(buf)
    {
        assert(buf->size() == 2*sizeof(std::uint64_t));
        std::copy_n(buf->data(), buf->size(), buf_->data());
    }

    operator std::shared_ptr<oncrpc::Buffer>() const
    {
        return buf_;
    }

    std::uint64_t id0() const
    {
        std::uint64_t n = 0;
        for (size_t i = 0; i < sizeof(n); i++) {
            n = (n << 8) + buf_->data()[i];
        }
        return n;
    }

    std::uint64_t id1() const
    {
        std::uint64_t n = 0;
        for (size_t i = 0; i < sizeof(n); i++) {
            n = (n << 8) + buf_->data()[8+i];
        }
        return n;
    }

private:
    std::shared_ptr<oncrpc::Buffer> buf_;
};

/// Key type for directory entries - we append the name to the fileid
/// which groups the keys for efficient lookup. We use big endian for
/// the fileid so we can iterate over the directory easily
struct DirectoryKeyType
{
    DirectoryKeyType(std::uint64_t id, std::string name)
        : buf_(std::make_shared<oncrpc::Buffer>(sizeof(std::uint64_t) + name.size()))
    {
        std::uint64_t n = id;
        for (size_t i = 0; i < sizeof(n); i++) {
            buf_->data()[i] = (n >> 56) & 0xff;
            n <<= 8;
        }
        std::copy_n(
            reinterpret_cast<const uint8_t*>(name.data()), name.size(),
            buf_->data() + sizeof(uint64_t));
    }

    DirectoryKeyType(std::shared_ptr<oncrpc::Buffer> buf)
        : buf_(buf)
    {
    }

    operator std::shared_ptr<oncrpc::Buffer>() const
    {
        return buf_;
    }

    std::uint64_t fileid() const
    {
        std::uint64_t n = 0;
        for (size_t i = 0; i < sizeof(n); i++) {
            n = (n << 8) + buf_->data()[i];
        }
        return n;
    }

    std::string name() const
    {
        return std::string(
            reinterpret_cast<const char*>(buf_->data() + sizeof(uint64_t)),
            buf_->size() - sizeof(uint64_t));
    }

private:
    std::shared_ptr<oncrpc::Buffer> buf_;
};

/// Key type for file data and block map - we index by fileid and byte offset,
/// using the highest bit of the file offset to distinguish between data and
/// block map
struct DataKeyType : public DoubleKeyType {
    DataKeyType(std::uint64_t id, std::uint64_t off)
        : DoubleKeyType(id, off)
    {
    }

    DataKeyType(std::shared_ptr<oncrpc::Buffer> buf)
        : DoubleKeyType(buf)
    {
    }

    FileId fileid() const
    {
        return FileId(id0());
    }

    std::uint64_t offset() const
    {
        return id1();
    }
};

}
}
