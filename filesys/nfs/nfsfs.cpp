#include <iomanip>
#include <sstream>

#include <fs++/filesys.h>
#include <fs++/urlparser.h>
#include <rpc++/channel.h>
#include <rpc++/client.h>
#include <rpc++/errors.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "nfsfs.h"
#include "filesys/pfs/pfsfs.h"
#include "filesys/nfs/mount.h"

using namespace filesys;
using namespace filesys::nfs;
using namespace std;

NfsFilesystem::NfsFilesystem(
    shared_ptr<INfsProgram3> proto,
    shared_ptr<detail::Clock> clock,
    nfs_fh3&& rootfh)
    : proto_(proto),
      clock_(clock),
      rootfh_(move(rootfh))
{
}

NfsFilesystem::~NfsFilesystem()
{
}

shared_ptr<File>
NfsFilesystem::root()
{
    if (!root_) {
        nfs_fh3 fh = rootfh_;
        root_ = find(move(fh));
    }
    return root_;
}

shared_ptr<NfsFile>
NfsFilesystem::find(nfs_fh3&& fh)
{
    auto res = proto_->getattr(GETATTR3args{fh});
    if (res.status == NFS3_OK)
        return find(move(fh), move(res.resok().obj_attributes));
    else
        throw system_error(res.status, system_category());
}

shared_ptr<NfsFile>
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
        auto file = make_shared<NfsFile>(
            shared_from_this(), move(fh), move(attr));
        auto p = lru_.insert(lru_.begin(), file);
        cache_[id] = p;
        return file;
    }
}

pair<shared_ptr<Filesystem>, string>
NfsFilesystemFactory::mount(const string& url)
{
    UrlParser p(url);
    LOG(INFO) << "Connecting to mount service on " << p.host;
    Mountprog3<oncrpc::SysClient> mountprog(p.host);

    auto& fsman = FilesystemManager::instance();
    auto pfs = fsman.mount<pfs::PfsFilesystem>(p.host + ":/");
    auto chan = oncrpc::Channel::open(url, "tcp");
    auto proto = make_shared<NfsProgram3<oncrpc::SysClient>>(chan);
    auto clock = make_shared<detail::SystemClock>();

    auto exports = mountprog.listexports();
    for (auto exp = exports.get(); exp; exp = exp->ex_next.get()) {
        LOG(INFO) << "mounting " << exp->ex_dir;
        auto mnt = mountprog.mnt(move(exp->ex_dir));
        if (mnt.fhs_status == MNT3_OK) {
            LOG(INFO) << "Accepted auth flavors: "
                      << mnt.mountinfo().auth_flavors;
            nfs_fh3 fh;
            fh.data = move(mnt.mountinfo().fhandle);
            pfs->add(
                exp->ex_dir,
                fsman.mount<NfsFilesystem>(
                    p.host + ":" + exp->ex_dir, proto, clock, move(fh)));
        }
    }

    return make_pair(pfs, p.path);
};

void filesys::nfs::init(FilesystemManager* fsman)
{
    fsman->add(make_shared<NfsFilesystemFactory>());
}
