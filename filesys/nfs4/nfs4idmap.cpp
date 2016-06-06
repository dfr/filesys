/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <chrono>
#include <mutex>
#include <unordered_map>
#include <pwd.h>
#include <grp.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "nfs4idmap.h"

DECLARE_string(realm);

using namespace filesys::nfs4;
using namespace std;

namespace {

static auto cacheTimeout = chrono::seconds(60);

class CacheMap
{
public:
    string operator[](int id)
    {
        unique_lock<mutex> lock(mutex_);
        auto i = idMap_.find(id);
        auto now = chrono::system_clock::now();
        if (i != idMap_.end()) {
            auto e = i->second;
            if (now - e->time > cacheTimeout) {
                valueMap_.erase(e->value);
                idMap_.erase(i);
            }
            else {
                return e->value;
            }
        }
        auto val = lookup(id);
        auto e = make_shared<entry>(entry{id, val, now});
        idMap_[id] = e;
        valueMap_[val] = e;
        return val;
    }

    int operator[](const string& val)
    {
        unique_lock<mutex> lock(mutex_);
        auto i = valueMap_.find(val);
        auto now = chrono::system_clock::now();
        if (i != valueMap_.end()) {
            auto e = i->second;
            if (now - e->time > cacheTimeout) {
                idMap_.erase(e->id);
                valueMap_.erase(i);
            }
            else {
                return e->id;
            }
        }
        auto id = lookup(val);
        auto e = make_shared<entry>(entry{id, val, now});
        idMap_[id] = e;
        valueMap_[val] = e;
        return id;
    }

    virtual string lookup(int id) = 0;
    virtual int lookup(const string& val) = 0;

private:
    struct entry {
        int id;
        string value;
        chrono::system_clock::time_point time;
    };
    mutex mutex_;
    unordered_map<int, shared_ptr<entry>> idMap_;
    unordered_map<string, shared_ptr<entry>> valueMap_;
};

class OwnerMap: public CacheMap
{
public:
    OwnerMap(const string& realm)
        : realm_(realm)
    {
    }

    string lookup(int id) override
    {

        ::passwd pbuf;
        char buf[128];
        ::passwd* pwd;
        if (::getpwuid_r(id, &pbuf, buf, sizeof(buf), &pwd) == 0) {
            if (pwd)
                return string(pwd->pw_name) + "@" + realm_;
        }
        return "unknown@" + realm_;
    }

    int lookup(const string& val) override
    {
        auto sep = val.find('@');
        if (sep == string::npos) {
            LOG(ERROR) << "Malformed owner: " << val;
            return 65534;
        }
        auto id = val.substr(0, sep);
        auto realm = val.substr(sep + 1);
        if (realm != realm_) {
            LOG(ERROR) << "Unknown realm for owner: " << val;
            return 65534;
        }

        ::passwd pbuf;
        char buf[128];
        ::passwd* pwd;
        if (::getpwnam_r(id.c_str(), &pbuf, buf, sizeof(buf), &pwd) == 0) {
            if (pwd)
                return pwd->pw_uid;
        }
        return 65534;
    }

private:
    string realm_;
};

class GroupMap: public CacheMap
{
public:
    GroupMap(const string& realm)
        : realm_(realm)
    {
    }

    string lookup(int id) override
    {
        ::group gbuf;
        char buf[128];
        ::group* grp;
        if (::getgrgid_r(id, &gbuf, buf, sizeof(buf), &grp) == 0) {
            if (grp)
                return string(grp->gr_name) + "@" + realm_;
        }
        return "unknown@" + realm_;
    }

    int lookup(const string& val) override
    {
        auto sep = val.find('@');
        if (sep == string::npos) {
            LOG(ERROR) << "Malformed owner: " << val;
            return 65534;
        }
        auto id = val.substr(0, sep);
        auto realm = val.substr(sep + 1);
        if (realm != realm_) {
            LOG(ERROR) << "Unknown realm for owner: " << val;
            return 65534;
        }

        ::group gbuf;
        char buf[128];
        ::group* grp;
        if (::getgrnam_r(id.c_str(), &gbuf, buf, sizeof(buf), &grp) == 0) {
            if (grp)
                return grp->gr_gid;
        }
        return 65534;
    }

private:
    string realm_;
};

}

class LocalIdMapperImpl: public IIdMapper
{
public:
    LocalIdMapperImpl()
        : owners_(FLAGS_realm),
          groups_(FLAGS_realm)
    {
    }

    int toUid(const string& s) override
    {
        return owners_[s];
    }

    string fromUid(int uid) override
    {
        return owners_[uid];
    }

    int toGid(const string& s) override
    {
        return groups_[s];
    }

    string fromGid(int uid) override
    {
        return groups_[uid];
    }

private:
    OwnerMap owners_;
    GroupMap groups_;
};

shared_ptr<IIdMapper> filesys::nfs4::LocalIdMapper()
{
    return make_shared<LocalIdMapperImpl>();
}
