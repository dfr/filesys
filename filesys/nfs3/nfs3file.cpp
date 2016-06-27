/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <fs++/filesys.h>
#include <fs++/urlparser.h>
#include <glog/logging.h>

#include "nfs3fs.h"

using namespace filesys;
using namespace filesys::nfs3;
using namespace std;

static system_error mapStatus(nfsstat3 stat)
{
    static unordered_map<int, int> statusMap = {
        { NFS3ERR_PERM, EPERM },
        { NFS3ERR_NOENT, ENOENT },
        { NFS3ERR_IO, EIO },
        { NFS3ERR_NXIO, ENXIO },
        { NFS3ERR_ACCES, EACCES },
        { NFS3ERR_EXIST, EEXIST },
        { NFS3ERR_XDEV, EXDEV },
        { NFS3ERR_NODEV, ENODEV },
        { NFS3ERR_NOTDIR, ENOTDIR },
        { NFS3ERR_ISDIR, EISDIR },
        { NFS3ERR_INVAL, EINVAL },
        { NFS3ERR_FBIG, EFBIG },
        { NFS3ERR_NOSPC, ENOSPC },
        { NFS3ERR_ROFS, EROFS },
        { NFS3ERR_MLINK, EMLINK },
        { NFS3ERR_NAMETOOLONG, ENAMETOOLONG },
        { NFS3ERR_NOTEMPTY, ENOTEMPTY },
        { NFS3ERR_DQUOT, EDQUOT },
        { NFS3ERR_STALE, ESTALE },
        { NFS3ERR_REMOTE, EREMOTE },
        { NFS3ERR_NOTSUPP, EOPNOTSUPP },
    };
    auto i = statusMap.find(int(stat));
    if (i != statusMap.end())
           return system_error(i->second, system_category());
    else
           return system_error(EINVAL, system_category());
}


NfsFile::NfsFile(
    shared_ptr<NfsFilesystem> fs, const nfs_fh3& fh, const fattr3& attr)
    : fs_(fs),
      fh_(fh),
      attrTime_(fs->clock()->now()),
      attr_(attr)
{
}

shared_ptr<Filesystem>
NfsFile::fs()
{
    return fs_.lock();
}

FileHandle
NfsFile::handle()
{
    throw system_error(EOPNOTSUPP, system_category());
}

bool NfsFile::access(const Credential& cred, int accmode)
{
    uint32_t flags = 0;
    if (accmode & AccessFlags::READ)
        flags |= ACCESS3_READ;
    if (accmode & AccessFlags::WRITE)
        flags |= ACCESS3_MODIFY;
    if (accmode & AccessFlags::EXECUTE) {
        if (attr_.type == NF3DIR)
            flags |= ACCESS3_LOOKUP;
        else
            flags |= ACCESS3_EXECUTE;
    }
    auto fs = fs_.lock();
    auto res = fs->proto()->access(ACCESS3args{fh_, flags});
    if (res.status == NFS3_OK) {
        update(res.resok().obj_attributes);
        return res.resok().access == flags;
    }
    else {
        update(res.resfail().obj_attributes);
        throw mapStatus(res.status);
    }
}

shared_ptr<Getattr>
NfsFile::getattr()
{
    auto fs = fs_.lock();
    auto now = fs->clock()->now();
    if (now - attrTime_ > ATTR_TIMEOUT) {
        auto res = fs->proto()->getattr(GETATTR3args{fh_});
        if (res.status == NFS3_OK) {
            attrTime_ = now;
            attr_ = res.resok().obj_attributes;
        }
        else {
            throw mapStatus(res.status);
        }
    }
    return make_shared<NfsGetattr>(attr_);
}

static inline int operator!=(const nfstime3& a, const nfstime3& b)
{
    return a.seconds != b.seconds || a.nseconds != b.nseconds;
}

void NfsFile::setattr(const Credential&, function<void(Setattr*)> cb)
{
    SETATTR3args args;
    args.object = fh_;

    auto& attr = args.new_attributes;
    NfsSetattr sattr(attr);
    cb(&sattr);

    // Don't bother to make the call if attributes haven't changed
    bool changed = false;
    if (attr.mode.set_it && attr.mode.mode() != attr_.mode)
        changed = true;
    if (attr.uid.set_it && attr.uid.uid() != attr_.uid)
        changed = true;
    if (attr.gid.set_it && attr.gid.gid() != attr_.gid)
        changed = true;
    if (attr.size.set_it && attr.size.size() != attr_.size)
        changed = true;
    if (attr.atime.set_it == SET_TO_CLIENT_TIME &&
        attr.atime.atime() != attr_.atime)
        changed = true;
    if (attr.mtime.set_it == SET_TO_CLIENT_TIME &&
        attr.mtime.mtime() != attr_.mtime)
        changed = true;
    if (!changed)
        return;

    args.guard.set_check(false);
    auto fs = fs_.lock();
    auto res = fs->proto()->setattr(args);
    if (res.status == NFS3_OK) {
        update(res.resok().obj_wcc.after);
    }
    else {
        update(res.resfail().obj_wcc.after);
        throw mapStatus(res.status);
    }
}

shared_ptr<File>
NfsFile::lookup(const Credential&, const string& name)
{
    return lookupInternal(name);
}

shared_ptr<OpenFile>
NfsFile::open(
    const Credential& cred, const string& name, int flags,
    function<void(Setattr*)> cb)
{
    if (flags & OpenFlags::CREATE) {
        CREATE3args args;
        args.where = {fh_, name};
        args.how.set_mode(
            (flags & OpenFlags::EXCLUSIVE) ? EXCLUSIVE : UNCHECKED);
        if (flags & OpenFlags::EXCLUSIVE) {
            uint64_t verf =
                chrono::system_clock::now().time_since_epoch().count();
            *reinterpret_cast<uint64_t*>(args.how.verf().data()) = verf;
        }
        else {
            NfsSetattr sattr(args.how.obj_attributes());
            cb(&sattr);
        }
        auto fs = fs_.lock();
        auto res = fs->proto()->create(args);
        if (res.status == NFS3_OK) {
            auto& resok = res.resok();
            update(resok.dir_wcc.after);
            auto f = find(name, resok.obj, resok.obj_attributes);
            if (flags & OpenFlags::TRUNCATE) {
                f->setattr(cred, [](auto attr) { attr->setSize(0); });
            }
            if (flags & OpenFlags::EXCLUSIVE) {
                f->setattr(cred, cb);
            }
            return make_shared<NfsOpenFile>(f);
        }
        else {
            update(res.resfail().dir_wcc.after);
            throw mapStatus(res.status);
        }
    }
    else {
        return make_shared<NfsOpenFile>(lookupInternal(name));
    }
}

std::shared_ptr<OpenFile> NfsFile::open(const Credential&, int)
{
    return make_shared<NfsOpenFile>(shared_from_this());
}


string
NfsFile::readlink(const Credential&)
{
    auto fs = fs_.lock();
    auto res = fs->proto()->readlink(READLINK3args{fh_});
    if (res.status == NFS3_OK) {
        update(res.resok().symlink_attributes);
        return res.resok().data;
    }
    else {
        update(res.resfail().symlink_attributes);
        throw mapStatus(res.status);
    }
}

shared_ptr<File>
NfsFile::mkdir(
    const Credential&, const string& name, function<void(Setattr*)> cb)
{
    MKDIR3args args;
    args.where = {fh_, name};
    NfsSetattr sattr(args.attributes);
    cb(&sattr);
    auto fs = fs_.lock();
    auto res = fs->proto()->mkdir(args);
    if (res.status == NFS3_OK) {
        auto& resok = res.resok();
        update(resok.dir_wcc.after);
        return find(name, resok.obj, resok.obj_attributes);
    }
    else {
        update(res.resfail().dir_wcc.after);
        throw mapStatus(res.status);
    }
}

shared_ptr<File> NfsFile::symlink(
    const Credential&, const string& name, const string& data,
    function<void(Setattr*)> cb)
{
    SYMLINK3args args;
    args.where = {fh_, name};
    args.symlink.symlink_data = data;
    NfsSetattr sattr(args.symlink.symlink_attributes);
    cb(&sattr);
    auto fs = fs_.lock();
    auto res = fs->proto()->symlink(args);
    if (res.status == NFS3_OK) {
        auto& resok = res.resok();
        update(resok.dir_wcc.after);
        return find(name, resok.obj, resok.obj_attributes);
    }
    else {
        update(res.resfail().dir_wcc.after);
        throw mapStatus(res.status);
    }
}

std::shared_ptr<File> NfsFile::mkfifo(
    const Credential&, const std::string& name,
    std::function<void(Setattr*)> cb)
{
    MKNOD3args args;
    args.where = {fh_, name};
    args.what.set_type(NF3FIFO);
    NfsSetattr attr(args.what.pipe_attributes());
    cb(&attr);
    auto fs = fs_.lock();
    auto res = fs->proto()->mknod(args);
    if (res.status == NFS3_OK) {
        auto& resok = res.resok();
        update(resok.dir_wcc.after);
        return find(name, resok.obj, resok.obj_attributes);
    }
    else {
        update(res.resfail().dir_wcc.after);
        throw mapStatus(res.status);
    }
}

void
NfsFile::remove(const Credential&, const string& name)
{
    auto fs = fs_.lock();
    auto res = fs->proto()->remove(REMOVE3args{{fh_, name}});
    if (res.status == NFS3_OK) {
        update(res.resok().dir_wcc.after);
    }
    else {
        update(res.resfail().dir_wcc.after);
        throw mapStatus(res.status);
    }
}

void
NfsFile::rmdir(const Credential&, const string& name)
{
    auto fs = fs_.lock();
    auto res = fs->proto()->rmdir(RMDIR3args{{fh_, name}});
    if (res.status == NFS3_OK) {
        update(res.resok().dir_wcc.after);
    }
    else {
        update(res.resfail().dir_wcc.after);
        throw mapStatus(res.status);
    }
}

void NfsFile::rename(
    const Credential&, const string& toName,
    shared_ptr<File> fromDir, const string& fromName)
{
    auto fs = fs_.lock();
    auto from = dynamic_cast<NfsFile*>(fromDir.get());
    auto res = fs->proto()->rename(RENAME3args{{from->fh_, fromName}, {fh_, toName}});
    if (res.status == NFS3_OK) {
        from->update(res.resok().fromdir_wcc.after);
        update(res.resok().todir_wcc.after);
    }
    else {
        from->update(res.resfail().fromdir_wcc.after);
        update(res.resfail().todir_wcc.after);
        throw mapStatus(res.status);
    }
}

void NfsFile::link(
    const Credential&, const std::string& name, std::shared_ptr<File> file)
{
    auto fs = fs_.lock();
    auto from = dynamic_cast<NfsFile*>(file.get());
    auto res = fs->proto()->link(LINK3args{from->fh_, {fh_, name}});
    if (res.status == NFS3_OK) {
        from->update(res.resok().file_attributes);
        update(res.resok().linkdir_wcc.after);
    }
    else {
        from->update(res.resfail().file_attributes);
        update(res.resfail().linkdir_wcc.after);
        throw mapStatus(res.status);
    }
}

shared_ptr<DirectoryIterator>
NfsFile::readdir(const Credential& cred, uint64_t seek)
{
    return make_shared<NfsDirectoryIterator>(cred, shared_from_this(), seek);
}

std::shared_ptr<Fsattr>
NfsFile::fsstat(const Credential&)
{
    auto fs = fs_.lock();
    auto statres = fs->proto()->fsstat(FSSTAT3args{fh_});
    if (statres.status != NFS3_OK) {
        update(statres.resfail().obj_attributes);
        throw system_error(statres.status, system_category());
    }
    auto pcres = fs->proto()->pathconf(PATHCONF3args{fh_});
    if (pcres.status != NFS3_OK) {
        update(pcres.resfail().obj_attributes);
        throw system_error(pcres.status, system_category());
    }
    update(statres.resok().obj_attributes);
    return make_shared<NfsFsattr>(statres.resok(), pcres.resok());
}

shared_ptr<NfsFile>
NfsFile::lookupInternal(const std::string& name)
{
    auto fs = fs_.lock();
    auto res = fs->proto()->lookup(LOOKUP3args{{fh_, name}});
    if (res.status == NFS3_OK) {
        auto& resok = res.resok();
        update(resok.dir_attributes);
        if (resok.obj_attributes.attributes_follow)
            return fs->find(
                resok.object,
                resok.obj_attributes.attributes());
        else
            return fs->find(resok.object);
    }
    else {
        update(res.resfail().dir_attributes);
        throw mapStatus(res.status);
    }
}

shared_ptr<NfsFile>
NfsFile::find(
    const string& name, const post_op_fh3& fh, const post_op_attr& attr)
{
    shared_ptr<NfsFile> f;
    if (!fh.handle_follows) {
        LOG(WARNING) << "no filehande returned from create-type RPC";
        f = lookupInternal(name);
    }
    else {
        auto fs = fs_.lock();
        if (attr.attributes_follow)
            f = fs->find(fh.handle(), attr.attributes());
        else
            f = fs->find(fh.handle());
    }
    return f;
}

void
NfsFile::update(const post_op_attr& attr)
{
    if (attr.attributes_follow) {
        update(attr.attributes());
    }
}

void
NfsFile::update(const fattr3& attr)
{
    attrTime_ = fs_.lock()->clock()->now();
    attr_ = attr;
}

shared_ptr<Buffer>
NfsOpenFile::read(uint64_t offset, uint32_t size, bool& eof)
{
    auto fs = file_->nfs();
    if (size > fs->fsinfo().rtpref)
        size = fs->fsinfo().rtpref;
    auto res = fs->proto()->read(READ3args{file_->fh(), offset, size});
    if (res.status == NFS3_OK) {
        file_->update(res.resok().file_attributes);
        eof = res.resok().eof;
        return move(res.resok().data);
    }
    else {
        file_->update(res.resfail().file_attributes);
        throw mapStatus(res.status);
    }
}

uint32_t
NfsOpenFile::write(uint64_t offset, shared_ptr<Buffer> data)
{
    auto fs = file_->nfs();
    if (data->size() > fs->fsinfo().wtpref) {
        data = make_shared<Buffer>(data, 0, fs->fsinfo().wtpref);
    }
    auto res = fs->proto()->write(
        WRITE3args{file_->fh(), offset, count3(data->size()), UNSTABLE, data});
    if (res.status == NFS3_OK) {
        // We ignore file_wcc.before since we aren't caching (yet)
        file_->update(res.resok().file_wcc.after);
        return res.resok().count;
    }
    else {
        throw mapStatus(res.status);
    }
}

void
NfsOpenFile::flush()
{
    auto fs = file_->nfs();
    auto res = fs->proto()->commit(COMMIT3args{file_->fh(), 0, 0});
    if (res.status == NFS3_OK) {
        file_->update(res.resok().file_wcc.after);
    }
    else {
        file_->update(res.resfail().file_wcc.after);
        throw mapStatus(res.status);
    }
}

