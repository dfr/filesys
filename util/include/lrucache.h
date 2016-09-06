/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

#include <cassert>
#include <list>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

namespace util {

/// A cache mapping instances of ID to shared_ptr<OBJ> entries
template <typename ID, typename OBJ,
          typename HASH = std::hash<ID>,
          typename EQUAL = std::equal_to<ID>>
class LRUCache
{
public:

    /// Find an entry in the cache for fileid. If the entry exists, call
    /// the update callback with the existing entry, otherwize the ctor
    /// callback is called to create a new entry.
    template <typename UPDATE, typename CTOR>
    std::shared_ptr<OBJ> find(const ID& fileid, UPDATE update, CTOR ctor)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto i = cache_.find(fileid);
        if (i != cache_.end()) {
            //VLOG(2) << "cache hit for fileid: " << fileid;
            auto p = i->second;
            lru_.splice(lru_.begin(), lru_, p);
            auto file = p->second;
            hits_++;
            update(file);
            return file;
        }
        else {
            misses_++;
            auto file = ctor(fileid);
            if (file)
                add(std::move(lock), fileid, file);
            return file;
        }
    }

    /// Add an entry to the cache
    void add(const ID& fileid, std::shared_ptr<OBJ> file)
    {
        add(std::unique_lock<std::mutex>(mutex_), fileid, file);
    }

    /// Remove a cache entry, returning the entry if it was present
    /// otherwise nullptr
    std::shared_ptr<OBJ> remove(const ID& fileid)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        std::shared_ptr<OBJ> obj;
        auto i = cache_.find(fileid);
        if (i != cache_.end()) {
            auto p = i->second;
            cache_.erase(fileid);
            obj = p->second;
            lru_.erase(p);
        }
        return obj;
    }

    /// Clear the cache
    void clear()
    {
        // Clearing cache_ must be first since it contains iterators
        // to lru_
        cache_.clear();
        lru_.clear();
    }

    /// Return the number of entries in the cache
    int size() const { return cache_.size(); }

    /// Return the maximum cache size
    auto sizeLimit() const { return sizeLimit_; }

    /// Set the cache size limit
    void setSizeLimit(int sz)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        sizeLimit_ = sz;
        expire(std::move(lock));
    }

    /// Return true if the cache contains this id
    bool contains(ID id)
    {
        return cache_.find(id) != cache_.end();
    }

    auto hits() const { return hits_; }
    auto misses() const { return misses_; }

    std::unique_lock<std::mutex> lock()
    {
        return std::unique_lock<std::mutex>(mutex_);
    }

    template <typename TAG>
    std::unique_lock<std::mutex> lock(TAG tag)
    {
        return std::unique_lock<std::mutex>(mutex_, tag);
    }

    // Support for iterating over the cace
    auto begin() { return lru_.begin(); }
    auto end() { return lru_.end(); }

private:

    void add(std::unique_lock<std::mutex>&& lock,
        const ID& fileid, std::shared_ptr<OBJ> file)
    {
        assert(lock);
        assert(cache_.find(fileid) == cache_.end());
        //VLOG(2) << "adding fileid: " << file->fileid();
        auto p = lru_.insert(lru_.begin(), std::make_pair(fileid, file));
        cache_[fileid] = p;
        expire(std::move(lock));
    }

    void expire(std::unique_lock<std::mutex>&& lock)
    {
        assert(lock);
        // Expire old entries if the cache is full
        while (cache_.size() > sizeLimit_) {
            bool expiredOne = false;
            for (auto i = lru_.rbegin(); i != lru_.rend(); ++i) {
                if (i->second.unique()) {
                    // This entry is only referenced by the cache so
                    // we can expire it.
                    const auto& oldest = *i;
                    //VLOG(2) << "expiring fileid: " << oldest->fileid();
                    auto p = cache_[oldest.first];
                    cache_.erase(oldest.first);
                    lru_.erase(p);
                    expiredOne = true;
                    break;
                }
            }
            // If the whole cache is busy, just let it grow
            if (!expiredOne)
                break;
        }
    }

    static constexpr int DEFAULT_SIZE_LIMIT = 1024;
    typedef std::list<std::pair<ID, std::shared_ptr<OBJ>>> lruT;

    // should be shared_timed_mutex but its missing on OS X
    std::mutex mutex_;
    lruT lru_;
    std::unordered_map<ID, typename lruT::iterator, HASH, EQUAL> cache_;
    int sizeLimit_ = DEFAULT_SIZE_LIMIT;
    int hits_ = 0;
    int misses_ = 0;
};

}
