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

#include <cassert>
#include <list>
#include <memory>

#include <rpc++/xdr.h>

namespace filesys {

using oncrpc::Buffer;

namespace detail {

/// Instances of this class are used to cache file data locally
class DataCache
{
public:
    enum State {
        /// Block was either read from the filesystem or was confirmed
        /// written to stable storage.
        STABLE,

        /// Block was written to the filesystem but may not be in
        /// stable storage.
        UNSTABLE,

        /// Block has not been written to the filesystem, e.g. in the
        /// case of locally cached writes.
        DIRTY
    };

    // Get data from the cache, if any. If any cached block overlaps
    // the request, return the overlapping segment, otherwise nullptr
    std::shared_ptr<Buffer>get(std::uint64_t offset, std::uint32_t count)
    {
        auto start = offset, end = offset + count;
        for (auto it = cache_.begin(); it != cache_.end(); ++it) {
            auto& b = *it;
            if (b.start >= end)
                break;
            if (b.start > start) {
                return nullptr;
            }
            if (b.end > start) {
                if (b.end < end) {
                    // Try to merge forwards
                    ++it;
                    while (it != cache_.end() && it->start == b.end) {
                        auto& nb = *it;
                        auto sz1 = b.end - b.start;
                        auto sz2 = nb.end - nb.start;
                        auto buf = std::make_shared<Buffer>(sz1 + sz2);
                        std::copy_n(b.data->data(), sz1, buf->data());
                        std::copy_n(nb.data->data(), sz2, buf->data() + sz1);
                        b.data = buf;
                        b.end = nb.end;
                        auto t = it;
                        ++it;
                        cache_.erase(t);
                    }
                    end = b.end;
                }
                return std::make_shared<Buffer>(
                    b.data, start - b.start, end - b.start);
            }
        }
        return nullptr;
    }

    // Add data to the cache
    void add(State state, std::uint64_t offset, std::shared_ptr<Buffer> data)
    {
        auto it = cache_.begin();

        Block newb { state, offset, offset + data->size(), data };

        // Skip blocks which are entirely before the new block
        while (it != cache_.end() && it->end <= newb.start)
            ++it;

        // Clip any blocks which overlap
        while (it != cache_.end() && it->start < newb.end) {
            auto& b = *it;
            if (b.start < newb.start) {
                if (b.end <= newb.end) {
                    // Case 1: Block starts before us but does not
                    // extend past us. We clip the block to our start
                    // offset. There may be following blocks which
                    // overlap so we can't insert.
                    b.data = std::make_shared<Buffer>(
                        b.data, 0, newb.start - b.start);
                    b.end = newb.start;
                    ++it;
                }
                else {
                    // Case 1: Block starts before us and extends past
                    // us. We must split it and insert the new block
                    // between the two pieces.
                    Block tail;
                    tail.start = newb.end;
                    tail.end = b.end;
                    tail.data = std::make_shared<Buffer>(
                        b.data, newb.end - b.start, b.end - b.start);
                    b.data = std::make_shared<Buffer>(
                        b.data, 0, newb.start - b.start);
                    b.end = newb.start;
                    ++it;
                    cache_.insert(it, { newb, tail });
                    return;
                }
            }
            else {
                if (b.end <= newb.end) {
                    // Case 3: Block is entirely covered by the new
                    // block. We discard the block and continue.
                    it = cache_.erase(it);
                }
                else {
                    // Case 4: Block ends after us - we clip and
                    // insert before it. Note: the while condition
                    // above means that we are sure it overlaps.
                    b.data = std::make_shared<Buffer>(
                        b.data, newb.end - b.start, b.end - b.start);
                    b.start = newb.end;
                    cache_.insert(it, newb);
                    return;
                }
            }
        }
        cache_.insert(it, newb);
    }

    template <typename FN>
    void apply(FN&& fn)
    {
        for (auto& b: cache_) {
            assert(b.data->size() == b.end - b.start);
            fn(b.state, b.start, b.end, b.data);
        }
    }

    void truncate(std::uint64_t size)
    {
        // Drop any blocks starting after the new size
        while (cache_.size() > 0) {
            auto& b = cache_.back();
            if (b.start < size) {
                break;
            }
            cache_.pop_back();
        }

        // If the last block extends past size, truncate it
        if (cache_.size() > 0) {
            auto& b = cache_.back();
            if (b.end > size) {
                b.end = size;
                b.data = std::make_shared<Buffer>(
                    b.data, 0, b.end - b.start);
            }
        }
    }

    void clear()
    {
        cache_.clear();
    }

    int blockCount() const { return cache_.size(); }

private:
    struct Block
    {
        State state;
        std::uint64_t start, end;
        std::shared_ptr<Buffer> data;
    };

    std::list<Block> cache_;
};

}
}
