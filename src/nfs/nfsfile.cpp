#include <fs++/filesys.h>
#include <fs++/nfsfs.h>
#include <fs++/urlparser.h>
#include <glog/logging.h>

#include "src/nfs/nfsfs.h"

using namespace filesys;
using namespace filesys::nfs;

NfsFile::NfsFile(
    std::shared_ptr<NfsFilesystem> fs, nfs_fh3&& fh, fattr3&& attr)
    : fs_(fs),
      fh_(std::move(fh)),
      attr_(std::move(attr))
{
}

std::shared_ptr<Getattr>
NfsFile::getattr()
{
    return std::make_shared<NfsGetattr>(attr_);
}

void NfsFile::setattr(std::function<void(Setattr*)> cb)
{
    SETATTR3args args;
    args.object = fh_;
    NfsSettattr sattr(args.new_attributes);
    cb(&sattr);
    args.guard.set_check(false);
    auto fs = fs_.lock();
    auto res = fs->setattr(std::move(args));
    if (res.status == NFS3_OK) {
        updateAttr(std::move(res.resok().obj_wcc.after));
    }
    else {
        updateAttr(std::move(res.resfail().obj_wcc.after));
        throw std::system_error(res.status, std::system_category());
    }
}

std::shared_ptr<File>
NfsFile::lookup(const std::string& name)
{
    auto fs = fs_.lock();
    auto res = fs->lookup(LOOKUP3args{{fh_, name}});
    if (res.status == NFS3_OK) {
        auto& resok = res.resok();
        updateAttr(std::move(resok.dir_attributes));
        if (resok.obj_attributes.attributes_follow)
            return fs->find(
                std::move(resok.object),
                std::move(resok.obj_attributes.attributes()));
        else
            return fs->find(std::move(resok.object));
    }
    else {
        updateAttr(std::move(res.resfail().dir_attributes));
        throw std::system_error(res.status, std::system_category());
    }
}

std::shared_ptr<File>
NfsFile::open(
    const std::string& name, int flags, std::function<void(Setattr*)> cb)
{
    if (flags & OpenFlags::CREATE) {
        CREATE3args args;
        args.where = {fh_, name};
        args.how.set_mode(
            (flags & OpenFlags::EXCLUSIVE) ? GUARDED : UNCHECKED);
        NfsSettattr sattr(args.how.obj_attributes());
        cb(&sattr);
        auto fs = fs_.lock();
        auto res = fs->create(std::move(args));
        if (res.status == NFS3_OK) {
            auto& resok = res.resok();
            if (!resok.obj.handle_follows) {
                LOG(ERROR) << "CREATE3res with no filehandle?";
                return lookup(name);
            }

            // XXX resfail().dir_wcc

            std::shared_ptr<NfsFile> f;
            if (resok.obj_attributes.attributes_follow)
                f = fs->find(
                    std::move(resok.obj.handle()),
                    std::move(resok.obj_attributes.attributes()));
            else
                f = fs->find(std::move(resok.obj.handle()));

            if ((flags & OpenFlags::TRUNCATE) && f->attr_.size != 0) {
                // XXX add File::setattr
                SETATTR3args args{f->fh(), {}};
                args.new_attributes.size.set_set_it(true);
                args.new_attributes.size.size() = 0;
                args.guard.set_check(false);
                auto res = fs->setattr(std::move(args));
                if (res.status == NFS3_OK) {
                    updateAttr(std::move(res.resok().obj_wcc.after));
                }
                else {
                    throw std::system_error(res.status, std::system_category());
                }
            }

            return f;
        }
        else {
            // XXX resfail().dir_wcc
            throw std::system_error(res.status, std::system_category());
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
        updateAttr(std::move(res.resok().file_wcc.after));
    }
    else {
        updateAttr(std::move(res.resfail().file_wcc.after));
        throw std::system_error(res.status, std::system_category());
    }
}

std::string
NfsFile::readlink()
{
    auto fs = fs_.lock();
    auto res = fs->readlink(READLINK3args{fh_});
    if (res.status == NFS3_OK) {
        return res.resok().data;
    }
    else {
        throw std::system_error(res.status, std::system_category());
    }
}

std::vector<std::uint8_t>
NfsFile::read(std::uint64_t offset, std::uint32_t size, bool& eof)
{
    auto fs = fs_.lock();
    auto res = fs->read(READ3args{fh_, offset, size});
    if (res.status == NFS3_OK) {
        updateAttr(std::move(res.resok().file_attributes));
        eof = res.resok().eof;
        return std::move(res.resok().data);
    }
    else {
        updateAttr(std::move(res.resfail().file_attributes));
        throw std::system_error(res.status, std::system_category());
    }
}

std::uint32_t
NfsFile::write(std::uint64_t offset, const std::vector<std::uint8_t>& data)
{
    auto fs = fs_.lock();
    auto res = fs->write(
        WRITE3args{fh_, offset, count3(data.size()), UNSTABLE, data});
    if (res.status == NFS3_OK) {
        // We ignore file_wcc.before since we aren't caching (yet)
        updateAttr(std::move(res.resok().file_wcc.after));
        return res.resok().count;
    }
    else {
        throw std::system_error(res.status, std::system_category());
    }
}

std::shared_ptr<File>
NfsFile::mkdir(const std::string& name, std::function<void(Setattr*)> cb)
{
    MKDIR3args args;
    args.where = {fh_, name};
    NfsSettattr sattr(args.attributes);
    cb(&sattr);
    auto fs = fs_.lock();
    auto res = fs->mkdir(std::move(args));
    if (res.status == NFS3_OK) {
        auto& resok = res.resok();
        updateAttr(std::move(resok.dir_wcc.after));

        if (!resok.obj.handle_follows) {
            LOG(ERROR) << "MKDIR3res with no filehandle?";
            return lookup(name);
        }

        if (resok.obj_attributes.attributes_follow)
            return fs->find(
                std::move(resok.obj.handle()),
                std::move(resok.obj_attributes.attributes()));
        else
            return fs->find(std::move(resok.obj.handle()));
    }
    else {
        updateAttr(std::move(res.resfail().dir_wcc.after));
        throw std::system_error(res.status, std::system_category());
    }
}

std::shared_ptr<DirectoryIterator>
NfsFile::readdir()
{
    return std::make_shared<NfsDirectoryIterator>(shared_from_this());
}

void
NfsFile::updateAttr(post_op_attr&& attr)
{
    if (attr.attributes_follow)
        attr_ = std::move(attr.attributes());
}
