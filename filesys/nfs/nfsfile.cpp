#include <fs++/filesys.h>
#include <fs++/urlparser.h>
#include <glog/logging.h>

#include "nfsfs.h"

using namespace filesys;
using namespace filesys::nfs;
using namespace std;

NfsFile::NfsFile(
    shared_ptr<NfsFilesystem> fs, nfs_fh3&& fh, fattr3&& attr)
    : fs_(fs),
      fh_(move(fh)),
      attrTime_(fs->clock()->now()),
      attr_(move(attr))
{
}

shared_ptr<Filesystem>
NfsFile::fs()
{
    return fs_.lock();
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
            throw system_error(res.status, system_category());
        }
    }
    return make_shared<NfsGetattr>(attr_);
}

static inline int operator!=(const nfstime3& a, const nfstime3& b)
{
    return a.seconds != b.seconds || a.nseconds != b.nseconds;
}

void NfsFile::setattr(function<void(Setattr*)> cb)
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
    auto res = fs->proto()->setattr(move(args));
    if (res.status == NFS3_OK) {
        update(move(res.resok().obj_wcc.after));
    }
    else {
        update(move(res.resfail().obj_wcc.after));
        throw system_error(res.status, system_category());
    }
}

shared_ptr<File>
NfsFile::lookup(const string& name)
{
    auto fs = fs_.lock();
    auto res = fs->proto()->lookup(LOOKUP3args{{fh_, name}});
    if (res.status == NFS3_OK) {
        auto& resok = res.resok();
        update(move(resok.dir_attributes));
        if (resok.obj_attributes.attributes_follow)
            return fs->find(
                move(resok.object),
                move(resok.obj_attributes.attributes()));
        else
            return fs->find(move(resok.object));
    }
    else {
        update(move(res.resfail().dir_attributes));
        throw system_error(res.status, system_category());
    }
}

shared_ptr<File>
NfsFile::open(
    const string& name, int flags, function<void(Setattr*)> cb)
{
    if (flags & OpenFlags::CREATE) {
        CREATE3args args;
        args.where = {fh_, name};
        args.how.set_mode(
            (flags & OpenFlags::EXCLUSIVE) ? GUARDED : UNCHECKED);
        NfsSetattr sattr(args.how.obj_attributes());
        cb(&sattr);
        auto fs = fs_.lock();
        auto res = fs->proto()->create(move(args));
        if (res.status == NFS3_OK) {
            auto& resok = res.resok();
            update(move(resok.dir_wcc.after));
            auto f = find(name, resok.obj, resok.obj_attributes);
            if (flags & OpenFlags::TRUNCATE) {
                f->setattr([](auto attr) { attr->setSize(0); });
            }
            return f;
        }
        else {
            update(move(res.resfail().dir_wcc.after));
            throw system_error(res.status, system_category());
        }
    }
    else {
        return lookup(name);
    }
}

void
NfsFile::close()
{
}

void
NfsFile::commit()
{
    auto fs = fs_.lock();
    auto res = fs->proto()->commit(COMMIT3args{fh_, 0, 0});
    if (res.status == NFS3_OK) {
        update(move(res.resok().file_wcc.after));
    }
    else {
        update(move(res.resfail().file_wcc.after));
        throw system_error(res.status, system_category());
    }
}

string
NfsFile::readlink()
{
    auto fs = fs_.lock();
    auto res = fs->proto()->readlink(READLINK3args{fh_});
    if (res.status == NFS3_OK) {
        return res.resok().data;
    }
    else {
        throw system_error(res.status, system_category());
    }
}

vector<uint8_t>
NfsFile::read(uint64_t offset, uint32_t size, bool& eof)
{
    auto fs = fs_.lock();
    auto res = fs->proto()->read(READ3args{fh_, offset, size});
    if (res.status == NFS3_OK) {
        update(move(res.resok().file_attributes));
        eof = res.resok().eof;
        return move(res.resok().data);
    }
    else {
        update(move(res.resfail().file_attributes));
        throw system_error(res.status, system_category());
    }
}

uint32_t
NfsFile::write(uint64_t offset, const vector<uint8_t>& data)
{
    auto fs = fs_.lock();
    auto res = fs->proto()->write(
        WRITE3args{fh_, offset, count3(data.size()), UNSTABLE, data});
    if (res.status == NFS3_OK) {
        // We ignore file_wcc.before since we aren't caching (yet)
        update(move(res.resok().file_wcc.after));
        return res.resok().count;
    }
    else {
        throw system_error(res.status, system_category());
    }
}

shared_ptr<File>
NfsFile::mkdir(const string& name, function<void(Setattr*)> cb)
{
    MKDIR3args args;
    args.where = {fh_, name};
    NfsSetattr sattr(args.attributes);
    cb(&sattr);
    auto fs = fs_.lock();
    auto res = fs->proto()->mkdir(move(args));
    if (res.status == NFS3_OK) {
        auto& resok = res.resok();
        update(move(resok.dir_wcc.after));
        return find(name, resok.obj, resok.obj_attributes);
    }
    else {
        update(move(res.resfail().dir_wcc.after));
        throw system_error(res.status, system_category());
    }
}

shared_ptr<File> NfsFile::symlink(
    const string& name, const string& data,
    function<void(Setattr*)> cb)
{
    SYMLINK3args args;
    args.where = {fh_, name};
    args.symlink.symlink_data = data;
    NfsSetattr sattr(args.symlink.symlink_attributes);
    cb(&sattr);
    auto fs = fs_.lock();
    auto res = fs->proto()->symlink(move(args));
    if (res.status == NFS3_OK) {
        auto& resok = res.resok();
        update(move(resok.dir_wcc.after));
        return find(name, resok.obj, resok.obj_attributes);
    }
    else {
        update(move(res.resfail().dir_wcc.after));
        throw system_error(res.status, system_category());
    }
}

std::shared_ptr<File> NfsFile::mkfifo(
    const std::string& name, std::function<void(Setattr*)> cb)
{
    MKNOD3args args;
    args.where = {fh_, name};
    args.what.set_type(NF3FIFO);
    NfsSetattr attr(args.what.pipe_attributes());
    cb(&attr);
    auto fs = fs_.lock();
    auto res = fs->proto()->mknod(move(args));
    if (res.status == NFS3_OK) {
        auto& resok = res.resok();
        update(move(resok.dir_wcc.after));
        return find(name, resok.obj, resok.obj_attributes);
    }
    else {
        update(move(res.resfail().dir_wcc.after));
        throw system_error(res.status, system_category());
    }
}

void
NfsFile::remove(const string& name)
{
    auto fs = fs_.lock();
    auto res = fs->proto()->remove(REMOVE3args{{fh_, name}});
    if (res.status == NFS3_OK) {
        update(move(res.resok().dir_wcc.after));
    }
    else {
        update(move(res.resfail().dir_wcc.after));
        throw system_error(res.status, system_category());
    }
}

void
NfsFile::rmdir(const string& name)
{
    auto fs = fs_.lock();
    auto res = fs->proto()->rmdir(RMDIR3args{{fh_, name}});
    if (res.status == NFS3_OK) {
        update(move(res.resok().dir_wcc.after));
    }
    else {
        update(move(res.resfail().dir_wcc.after));
        throw system_error(res.status, system_category());
    }
}

void NfsFile::rename(
    const string& toName, shared_ptr<File> fromDir, const string& fromName)
{
    auto fs = fs_.lock();
    auto from = dynamic_cast<NfsFile*>(fromDir.get());
    auto res = fs->proto()->rename(RENAME3args{{from->fh_, fromName}, {fh_, toName}});
    if (res.status == NFS3_OK) {
        from->update(move(res.resok().fromdir_wcc.after));
        update(move(res.resok().todir_wcc.after));
    }
    else {
        from->update(move(res.resfail().fromdir_wcc.after));
        update(move(res.resfail().todir_wcc.after));
        throw system_error(res.status, system_category());
    }
}

void NfsFile::link(const std::string& name, std::shared_ptr<File> file)
{
    auto fs = fs_.lock();
    auto from = dynamic_cast<NfsFile*>(file.get());
    auto res = fs->proto()->link(LINK3args{from->fh_, {fh_, name}});
    if (res.status == NFS3_OK) {
        from->update(move(res.resok().file_attributes));
        update(move(res.resok().linkdir_wcc.after));
    }
    else {
        from->update(move(res.resfail().file_attributes));
        update(move(res.resfail().linkdir_wcc.after));
        throw system_error(res.status, system_category());
    }
}

shared_ptr<DirectoryIterator>
NfsFile::readdir()
{
    return make_shared<NfsDirectoryIterator>(shared_from_this());
}

std::shared_ptr<Fsattr>
NfsFile::fsstat()
{
    auto fs = fs_.lock();
    auto res = fs->proto()->fsstat(FSSTAT3args{fh_});
    if (res.status == NFS3_OK) {
        update(move(res.resok().obj_attributes));
        return make_shared<NfsFsattr>(res.resok());
    }
    else {
        update(move(res.resfail().obj_attributes));
        throw system_error(res.status, system_category());
    }
}

shared_ptr<File>
NfsFile::find(const string& name, post_op_fh3& fh, post_op_attr& attr)
{
    shared_ptr<File> f;
    if (!fh.handle_follows) {
        LOG(WARNING) << "no filehande returned from create-type RPC";
        f = lookup(name);
    }
    else {
        auto fs = fs_.lock();
        if (attr.attributes_follow)
            f = fs->find(
                move(fh.handle()),
                move(attr.attributes()));
        else
            f = fs->find(move(fh.handle()));
    }
    return move(f);
}

void
NfsFile::update(post_op_attr&& attr)
{
    if (attr.attributes_follow) {
        attrTime_ = fs_.lock()->clock()->now();
        attr_ = move(attr.attributes());
    }
}
