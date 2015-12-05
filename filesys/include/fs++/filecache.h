#pragma once

#include <cassert>
#include <list>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

namespace filesys {
namespace detail {

/// A cache mapping instances of ID to shared_ptr<FILE> entries
template <typename ID, typename FILE, typename HASH = std::hash<ID>>
class FileCache
{
public:

    /// Find an entry in the cache for fileid. If the entry exists, call
    /// the update callback with the existing entry, otherwize the ctor
    /// callback is called to create a new entry.
    template <typename UPDATE, typename CTOR>
    std::shared_ptr<FILE> find(const ID& fileid, UPDATE update, CTOR ctor)
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
            add(std::move(lock), fileid, file);
            return file;
        }
    }

    /// Add an entry to the cache
    void add(const ID& fileid, std::shared_ptr<FILE> file)
    {
        add(std::unique_lock<std::mutex>(mutex_), fileid, file);
    }

    /// Remove a cache entry
    void remove(const ID& fileid)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto i = cache_.find(fileid);
        if (i != cache_.end()) {
            cache_.erase(fileid);
            lru_.erase(i->second);
        }
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

private:

    void add(std::unique_lock<std::mutex>&& lock,
        const ID& fileid, std::shared_ptr<FILE> file)
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
            const auto& oldest = lru_.back();
            //VLOG(2) << "expiring fileid: " << oldest->fileid();
            cache_.erase(oldest.first);
            lru_.pop_back();
        }
    }

    static constexpr int DEFAULT_SIZE_LIMIT = 1024;
    typedef std::list<std::pair<ID, std::shared_ptr<FILE>>> lruT;

    // should be shared_timed_mutex but its missing on OS X
    std::mutex mutex_;
    lruT lru_;
    std::unordered_map<ID, typename lruT::iterator, HASH> cache_;
    int sizeLimit_ = DEFAULT_SIZE_LIMIT;
    int hits_ = 0;
    int misses_ = 0;
};

}
}
