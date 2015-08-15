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
    return make_shared<NfsGetattr>(attr_);
}

void NfsFile::setattr(function<void(Setattr*)> cb)
{
    SETATTR3args args;
    args.object = fh_;
    NfsSetattr sattr(args.new_attributes);
    cb(&sattr);
    args.guard.set_check(false);
    auto fs = fs_.lock();
    auto res = fs->setattr(move(args));
    if (res.status == NFS3_OK) {
        updateAttr(move(res.resok().obj_wcc.after));
    }
    else {
        updateAttr(move(res.resfail().obj_wcc.after));
        throw system_error(res.status, system_category());
    }
}

shared_ptr<File>
NfsFile::lookup(const string& name)
{
    auto fs = fs_.lock();
    auto res = fs->lookup(LOOKUP3args{{fh_, name}});
    if (res.status == NFS3_OK) {
        auto& resok = res.resok();
        updateAttr(move(resok.dir_attributes));
        if (resok.obj_attributes.attributes_follow)
            return fs->find(
                move(resok.object),
                move(resok.obj_attributes.attributes()));
        else
            return fs->find(move(resok.object));
    }
    else {
        updateAttr(move(res.resfail().dir_attributes));
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
        auto res = fs->create(move(args));
        if (res.status == NFS3_OK) {
            auto& resok = res.resok();
            if (!resok.obj.handle_follows) {
                LOG(ERROR) << "CREATE3res with no filehandle?";
                return lookup(name);
            }

            // XXX resfail().dir_wcc

            shared_ptr<NfsFile> f;
            if (resok.obj_attributes.attributes_follow)
                f = fs->find(
                    move(resok.obj.handle()),
                    move(resok.obj_attributes.attributes()));
            else
                f = fs->find(move(resok.obj.handle()));

            if ((flags & OpenFlags::TRUNCATE) && f->attr_.size != 0) {
                // XXX add File::setattr
                SETATTR3args args{f->fh(), {}};
                NfsSetattr sattr(args.new_attributes);
                sattr.setSize(0);
                args.guard.set_check(false);
                auto res = fs->setattr(move(args));
                if (res.status == NFS3_OK) {
                    updateAttr(move(res.resok().obj_wcc.after));
                }
                else {
                    throw system_error(res.status, system_category());
                }
            }

            return f;
        }
        else {
            // XXX resfail().dir_wcc
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
    auto res = fs->commit(COMMIT3args{fh_, 0, 0});
    if (res.status == NFS3_OK) {
        updateAttr(move(res.resok().file_wcc.after));
    }
    else {
        updateAttr(move(res.resfail().file_wcc.after));
        throw system_error(res.status, system_category());
    }
}

string
NfsFile::readlink()
{
    auto fs = fs_.lock();
    auto res = fs->readlink(READLINK3args{fh_});
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
    auto res = fs->read(READ3args{fh_, offset, size});
    if (res.status == NFS3_OK) {
        updateAttr(move(res.resok().file_attributes));
        eof = res.resok().eof;
        return move(res.resok().data);
    }
    else {
        updateAttr(move(res.resfail().file_attributes));
        throw system_error(res.status, system_category());
    }
}

uint32_t
NfsFile::write(uint64_t offset, const vector<uint8_t>& data)
{
    auto fs = fs_.lock();
    auto res = fs->write(
        WRITE3args{fh_, offset, count3(data.size()), UNSTABLE, data});
    if (res.status == NFS3_OK) {
        // We ignore file_wcc.before since we aren't caching (yet)
        updateAttr(move(res.resok().file_wcc.after));
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
    auto res = fs->mkdir(move(args));
    if (res.status == NFS3_OK) {
        auto& resok = res.resok();
        updateAttr(move(resok.dir_wcc.after));

        if (!resok.obj.handle_follows) {
            LOG(ERROR) << "MKDIR3res with no filehandle?";
            return lookup(name);
        }

        if (resok.obj_attributes.attributes_follow)
            return fs->find(
                move(resok.obj.handle()),
                move(resok.obj_attributes.attributes()));
        else
            return fs->find(move(resok.obj.handle()));
    }
    else {
        updateAttr(move(res.resfail().dir_wcc.after));
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
    auto res = fs->symlink(move(args));
    if (res.status == NFS3_OK) {
        auto& resok = res.resok();
        updateAttr(move(resok.dir_wcc.after));

        if (!resok.obj.handle_follows) {
            LOG(ERROR) << "SYMLINK3res with no filehandle?";
            return lookup(name);
        }

        if (resok.obj_attributes.attributes_follow)
            return fs->find(
                move(resok.obj.handle()),
                move(resok.obj_attributes.attributes()));
        else
            return fs->find(move(resok.obj.handle()));
    }
    else {
        updateAttr(move(res.resfail().dir_wcc.after));
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
    auto res = fs->mknod(move(args));
    if (res.status == NFS3_OK) {
        auto& resok = res.resok();
        updateAttr(move(resok.dir_wcc.after));

        if (!resok.obj.handle_follows) {
            LOG(ERROR) << "MKNOD3res with no filehandle?";
            return lookup(name);
        }

        if (resok.obj_attributes.attributes_follow)
            return fs->find(
                move(resok.obj.handle()),
                move(resok.obj_attributes.attributes()));
        else
            return fs->find(move(resok.obj.handle()));
    }
    else {
        updateAttr(move(res.resfail().dir_wcc.after));
        throw system_error(res.status, system_category());
    }
}

void
NfsFile::remove(const string& name)
{
    auto fs = fs_.lock();
    auto res = fs->remove(REMOVE3args{{fh_, name}});
    if (res.status == NFS3_OK) {
        updateAttr(move(res.resok().dir_wcc.after));
    }
    else {
        updateAttr(move(res.resfail().dir_wcc.after));
        throw system_error(res.status, system_category());
    }
}

void
NfsFile::rmdir(const string& name)
{
    auto fs = fs_.lock();
    auto res = fs->rmdir(RMDIR3args{{fh_, name}});
    if (res.status == NFS3_OK) {
        updateAttr(move(res.resok().dir_wcc.after));
    }
    else {
        updateAttr(move(res.resfail().dir_wcc.after));
        throw system_error(res.status, system_category());
    }
}

void NfsFile::rename(
    const string& toName, shared_ptr<File> fromDir, const string& fromName)
{
    auto fs = fs_.lock();
    auto from = dynamic_cast<NfsFile*>(fromDir.get());
    auto res = fs->rename(RENAME3args{{from->fh_, fromName}, {fh_, toName}});
    if (res.status == NFS3_OK) {
        from->updateAttr(move(res.resok().fromdir_wcc.after));
        updateAttr(move(res.resok().todir_wcc.after));
    }
    else {
        from->updateAttr(move(res.resfail().fromdir_wcc.after));
        updateAttr(move(res.resfail().todir_wcc.after));
        throw system_error(res.status, system_category());
    }
}

void NfsFile::link(const std::string& name, std::shared_ptr<File> file)
{
    auto fs = fs_.lock();
    auto from = dynamic_cast<NfsFile*>(file.get());
    auto res = fs->link(LINK3args{from->fh_, {fh_, name}});
    if (res.status == NFS3_OK) {
        from->updateAttr(move(res.resok().file_attributes));
        updateAttr(move(res.resok().linkdir_wcc.after));
    }
    else {
        from->updateAttr(move(res.resfail().file_attributes));
        updateAttr(move(res.resfail().linkdir_wcc.after));
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
    auto res = fs->fsstat(FSSTAT3args{fh_});
    if (res.status == NFS3_OK) {
        updateAttr(move(res.resok().obj_attributes));
        return make_shared<NfsFsattr>(res.resok());
    }
    else {
        updateAttr(move(res.resfail().obj_attributes));
        throw system_error(res.status, system_category());
    }
}

void
NfsFile::updateAttr(post_op_attr&& attr)
{
    if (attr.attributes_follow)
        attr_ = move(attr.attributes());
}
