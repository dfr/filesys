#include <iomanip>
#include <sstream>

#include <fs++/filesys.h>
#include <fs++/nfsfs.h>
#include <fs++/urlparser.h>
#include <rpc++/channel.h>
#include <rpc++/client.h>
#include <rpc++/errors.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "src/nfs/nfsfs.h"
#include "src/nfs/mount.h"
#include "src/pfs/pfsfs.h"

using namespace filesys;
using namespace filesys::nfs;

NfsFilesystem::NfsFilesystem(
    std::shared_ptr<oncrpc::Channel> chan, nfs_fh3&& rootfh)
    : NfsProgram3<oncrpc::SysClient>(chan),
      rootfh_(std::move(rootfh))
{
}

std::shared_ptr<File>
NfsFilesystem::root()
{
    nfs_fh3 fh = rootfh_;
    return find(std::move(fh));
};

std::shared_ptr<NfsFile>
NfsFilesystem::find(nfs_fh3&& fh)
{
    auto res = getattr(GETATTR3args{fh});
    if (res.status == NFS3_OK)
        return find(std::move(fh), std::move(res.resok().obj_attributes));
    else
        throw std::system_error(res.status, std::system_category());
}

std::shared_ptr<NfsFile>
NfsFilesystem::find(nfs_fh3&& fh, fattr3&& attr)
{
    auto id = attr.fileid;
    auto i = cache_.find(id);
    if (i != cache_.end()) {
        VLOG(2) << "cache hit for fileid: " << id;
        auto p = i->second;
        lru_.splice(lru_.begin(), lru_, p);
        return *p;
    }
    else {
        // Expire old entries if the cache is full
        if (cache_.size() == maxCache_) {
            auto oldest = lru_.back();
            VLOG(2) << "expiring fileid: " << oldest->fileid();
            cache_.erase(oldest->fileid());
            lru_.pop_back();
        }
        VLOG(2) << "adding fileid: " << id;
        auto file = std::make_shared<NfsFile>(
            shared_from_this(), std::move(fh), std::move(attr));
        auto p = lru_.insert(lru_.begin(), file);
        cache_[id] = p;
        return file;
    }
}

std::shared_ptr<Filesystem>
NfsFilesystemFactory::mount(const std::string& url)
{
    auto pfs = std::make_shared<pfs::PfsFilesystem>();
    auto chan = oncrpc::Channel::open(url, "tcp");

    UrlParser p(url);

    LOG(INFO) << "Connecting to mount service on " << p.host;
    Mountprog3<oncrpc::SysClient> mountprog(p.host);

    auto exports = mountprog.listexports();
    for (auto p = exports.get(); p; p = p->ex_next.get()) {
        LOG(INFO) << "mounting " << p->ex_dir;
        auto mnt = mountprog.mnt(std::move(p->ex_dir));
        if (mnt.fhs_status == MNT3_OK) {
            LOG(INFO) << "Accepted auth flavors: "
                      << mnt.mountinfo().auth_flavors;
            nfs_fh3 fh;
            fh.data = std::move(mnt.mountinfo().fhandle);
            pfs->add(
                p->ex_dir,
                std::make_shared<NfsFilesystem>(chan, std::move(fh)));
        }
    }

    return pfs;
};
