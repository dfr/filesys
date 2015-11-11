#pragma once

#include <list>
#include <memory>
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
        auto i = cache_.find(fileid);
        if (i != cache_.end()) {
            //VLOG(2) << "cache hit for fileid: " << fileid;
            auto p = i->second;
            lru_.splice(lru_.begin(), lru_, p);
            auto file = p->second;
            update(file);
            return file;
        }
        else {
            auto file = ctor(fileid);
            add(fileid, file);
            return file;
        }
    }

    /// Add an entry to the cache
    void add(const ID& fileid, std::shared_ptr<FILE> file)
    {
        assert(cache_.find(fileid) == cache_.end());
        //VLOG(2) << "adding fileid: " << file->fileid();
        auto p = lru_.insert(lru_.begin(), std::make_pair(fileid, file));
        cache_[fileid] = p;
        expire();
    }

    /// Return the number of entries in the cache
    int size() const { return cache_.size(); }

    /// Return the maximum cache size
    auto sizeLimit() const { return sizeLimit_; }

    /// Set the cache size limit
    void setSizeLimit(int sz)
    {
        sizeLimit_ = sz;
        expire();
    }

    /// Return true if the cache contains this id
    bool contains(ID id)
    {
        return cache_.find(id) != cache_.end();
    }

private:
    void expire()
    {
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
    lruT lru_;
    std::unordered_map<ID, typename lruT::iterator, HASH> cache_;
    int sizeLimit_ = DEFAULT_SIZE_LIMIT;
};

}
}
