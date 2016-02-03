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
        for (auto& b: cache_) {
            if (b.start >= end)
                break;
            if (b.end > start) {
                if (b.end < end)
                    end = b.end;
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
