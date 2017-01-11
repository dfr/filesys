/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#include <iomanip>
#include <sstream>

#include <filesys/filesys.h>
#include <rpc++/channel.h>
#include <rpc++/client.h>
#include <rpc++/errors.h>
#include <rpc++/urlparser.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "nfs3fs.h"
#include "filesys/pfs/pfsfs.h"
#include "filesys/proto/mount.h"

using namespace filesys;
using namespace filesys::nfs3;
using namespace std;

DEFINE_int32(mount_port, 0, "port use for contacting mount service");

NfsFilesystem::NfsFilesystem(
    shared_ptr<INfsProgram3> proto,
    shared_ptr<util::Clock> clock,
    nfs_fh3&& rootfh)
    : proto_(proto),
      clock_(clock),
      rootfh_(rootfh)
{
}

NfsFilesystem::~NfsFilesystem()
{
}

shared_ptr<File>
NfsFilesystem::root()
{
    if (!root_) {
        root_ = find(rootfh_);

        auto res = proto_->fsinfo(FSINFO3args{rootfh_});
        if (res.status != NFS3_OK)
            throw system_error(res.status, system_category());
        fsinfo_.rtmax = res.resok().rtmax;
        fsinfo_.rtpref = res.resok().rtpref;
        fsinfo_.rtmult = res.resok().rtmult;
        fsinfo_.wtmax = res.resok().wtmax;
        fsinfo_.wtpref = res.resok().wtpref;
        fsinfo_.wtmult = res.resok().wtmult;
        fsinfo_.dtpref = res.resok().dtpref;
        fsinfo_.maxfilesize = res.resok().maxfilesize;
        fsinfo_.timedelta = res.resok().time_delta;
        fsinfo_.properties = res.resok().properties;

        // Set the buffer size for the largest read or write request we will
        // make, allowing extra space for protocol overhead
        proto_->setBufferSize(
            512 + max(fsinfo_.rtpref, max(fsinfo_.wtpref, fsinfo_.dtpref)));
    }
    return root_;
}

const FilesystemId&
NfsFilesystem::fsid() const
{
    static FilesystemId nullfsid;
    return nullfsid;
}

shared_ptr<File>
NfsFilesystem::find(const FileHandle& fh)
{
    throw system_error(ESTALE, system_category());
}

shared_ptr<NfsFile>
NfsFilesystem::find(const nfs_fh3& fh)
{
    auto res = proto_->getattr(GETATTR3args{fh});
    if (res.status == NFS3_OK)
        return find(fh, res.resok().obj_attributes);
    else
        throw system_error(res.status, system_category());
}

shared_ptr<NfsFile>
NfsFilesystem::find(const nfs_fh3& fh, const fattr3& attr)
{
    return cache_.find(
        fh,
        [&](auto file) {
            file->update(attr);
        },
        [&](const nfs_fh3& fh) {
            return make_shared<NfsFile>(
                shared_from_this(), fh, attr);
        });
}

shared_ptr<Filesystem>
NfsFilesystemFactory::mount(const string& url)
{
    using namespace oncrpc;

    static map<int, string> flavors {
        { AUTH_NONE, "none" },
        { AUTH_SYS, "sys" },
        { RPCSEC_GSS_KRB5, "krb5" },
        { RPCSEC_GSS_KRB5I, "krb5i" },
        { RPCSEC_GSS_KRB5P, "krb5p" },
    };

    UrlParser p(url);
    LOG(INFO) << "Connecting to mount service on " << p.host;
    shared_ptr<Channel> mchan;
    if (FLAGS_mount_port) {
        mchan = Channel::open(p.host, to_string(FLAGS_mount_port), "tcp");
    }
    else {
        mchan = Channel::open(p.host, MOUNTPROG, MOUNTVERS, "tcp");
    }
    Mountprog3<SysClient> mountprog(mchan);

    auto pfs = make_shared<pfs::PfsFilesystem>();
    auto chan = Channel::open(url, "tcp");
    auto clock = make_shared<util::SystemClock>();

    auto exports = mountprog.listexports();
    string path = p.path;
    for (auto exp = exports.get(); exp; exp = exp->ex_next.get()) {
        if (path.size() > 0 && exp->ex_dir != path)
            continue;
        LOG(INFO) << "mounting " << exp->ex_dir;
        auto mnt = mountprog.mnt(move(exp->ex_dir));
        if (mnt.fhs_status == MNT3_OK) {
            vector<string> flavorNames;
            for (auto flavor: mnt.mountinfo().auth_flavors) {
                auto p = flavors.find(flavor);
                flavorNames.push_back(
                    p == flavors.end() ? to_string(flavor) : p->second);
            }
            LOG(INFO) << "Accepted auth flavors: " << flavorNames;
            nfs_fh3 fh;
            fh.data = move(mnt.mountinfo().fhandle);

            // Just pick the first listed flavor
            shared_ptr<INfsProgram3> proto;
            if (mnt.mountinfo().auth_flavors.size() == 0) {
                LOG(ERROR) << "No auth flavors?";
                continue;
            }
            switch (mnt.mountinfo().auth_flavors[0]) {
            case AUTH_SYS:
                proto = make_shared<NfsProgram3<SysClient>>(chan);
                break;

            case RPCSEC_GSS_KRB5:
                proto = make_shared<NfsProgram3<GssClient>>(
                    chan, "nfs@" + p.host, "krb5", GssService::NONE);
                break;

            case RPCSEC_GSS_KRB5I:
                proto = make_shared<NfsProgram3<GssClient>>(
                    chan, "nfs@" + p.host, "krb5", GssService::INTEGRITY);
                break;

            case RPCSEC_GSS_KRB5P:
                proto = make_shared<NfsProgram3<GssClient>>(
                    chan, "nfs@" + p.host, "krb5", GssService::PRIVACY);
                break;

            default:
                LOG(ERROR) << "Unsupported auth flavor: "
                           << mnt.mountinfo().auth_flavors[0];
                continue;
            }

            auto fs = make_shared<NfsFilesystem>(proto, clock, move(fh));
            pfs->add(exp->ex_dir, fs);
        }
    }

    return pfs;
}
