#include <iomanip>
#include <sstream>

#include <fs++/filesys.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "nfs3server.h"

using namespace filesys;
using namespace filesys::nfs;
using namespace nfsd;
using namespace nfsd::nfs3;
using namespace oncrpc;
using namespace std;
using namespace std::chrono;

DECLARE_int32(iosize);

static nfsstat3 exportStatus(const system_error& e)
{
    // XXX: translate errors
    return nfsstat3(e.code().value());
}

static string formatFileHandle(const nfs_fh3& nfh)
{
    ostringstream ss;
    ss << "{";
    for (auto b: nfh.data)
        ss << hex << setw(2) << setfill('0') << int(b);
    ss << "}";
    return ss.str();
}

static shared_ptr<File> importFileHandle(const nfs_fh3& nfh)
{
    FileHandle fh;
    try {
        XdrMemory xm(const_cast<uint8_t*>(nfh.data.data()), nfh.data.size());
        xdr(fh, static_cast<XdrSource*>(&xm));
        if (fh.version != 1) {
            LOG(ERROR) << "unexpected file handle version: "
                       << fh.version << ", expected 1";
            throw system_error(ESTALE, system_category());
        }
    }
    catch (XdrError&) {
        throw system_error(ESTALE, system_category());
    }
    return FilesystemManager::instance().find(fh);
}

static nfs_fh3 exportFileHandle(shared_ptr<File> file)
{
    FileHandle fh;
    nfs_fh3 nfh;

    file->handle(fh);
    nfh.data.resize(XdrSizeof(fh));
    XdrMemory xm(nfh.data.data(), nfh.data.size());
    xdr(fh, static_cast<XdrSink*>(&xm));

    return nfh;
}

static auto importTime(const nfstime3& t)
{
    auto d = seconds(t.seconds) + nanoseconds(t.nseconds);
    return system_clock::time_point(duration_cast<system_clock::duration>(d));
}

static auto exportTime(system_clock::time_point time)
{
    auto d = time.time_since_epoch();
    auto sec = duration_cast<seconds>(d);
    auto nsec = duration_cast<nanoseconds>(d) - sec;
    return nfstime3{uint32(sec.count()), uint32(nsec.count())};
}

static auto importAttr(const sattr3& attr)
{
    return [&](auto sattr)
    {
        if (attr.mode.set_it) {
            sattr->setMode(attr.mode.mode());
        }
        if (attr.uid.set_it) {
            sattr->setUid(attr.uid.uid());
        }
        if (attr.gid.set_it) {
            sattr->setUid(attr.gid.gid());
        }
        if (attr.size.set_it) {
            sattr->setSize(attr.size.size());
        }
        switch (attr.atime.set_it) {
        case DONT_CHANGE:
            break;
        case SET_TO_SERVER_TIME:
            sattr->setAtime(system_clock::now());
            break;
        case SET_TO_CLIENT_TIME:
            sattr->setAtime(importTime(attr.atime.atime()));
            break;
        }
        switch (attr.mtime.set_it) {
        case DONT_CHANGE:
            break;
        case SET_TO_SERVER_TIME:
            sattr->setMtime(system_clock::now());
            break;
        case SET_TO_CLIENT_TIME:
            sattr->setMtime(importTime(attr.mtime.mtime()));
            break;
        }
    };
}

static fattr3 exportAttr(shared_ptr<File> file)
{
    fattr3 res;
    auto attr = file->getattr();
    switch (attr->type()) {
    case FileType::FILE:
        res.type = NF3REG;
        break;
    case FileType::DIRECTORY:
        res.type = NF3DIR;
        break;
    case FileType::BLOCKDEV:
        res.type = NF3BLK;
        break;
    case FileType::CHARDEV:
        res.type = NF3CHR;
        break;
    case FileType::SYMLINK:
        res.type = NF3LNK;
        break;
    case FileType::SOCKET:
        res.type = NF3SOCK;
        break;
    case FileType::FIFO:
        res.type = NF3FIFO;
        break;
    }
    res.mode = attr->mode();
    res.nlink = attr->nlink();
    res.uid = attr->uid();
    res.gid = attr->gid();
    res.size = attr->size();
    res.used = attr->used();
    res.rdev = specdata3{0, 0},
    res.fsid = 0,
    res.fileid = attr->fileid();
    res.atime = exportTime(attr->atime());
    res.mtime = exportTime(attr->mtime());
    res.ctime = exportTime(attr->ctime());
    return res;
}

static wcc_attr exportWcc(shared_ptr<File> file)
{
    auto attr = file->getattr();
    return wcc_attr{
        attr->size(),
        exportTime(attr->mtime()),
        exportTime(attr->ctime())};
}

NfsServer::NfsServer()
{
}

// INfsProgram3 overrides
void NfsServer::null()
{
}

GETATTR3res NfsServer::getattr(const GETATTR3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::getattr("
                << formatFileHandle(args.object) << ")";
    try {
        auto obj = importFileHandle(args.object);
        return GETATTR3res{
            NFS3_OK,
            GETATTR3resok{exportAttr(obj)}};
    }
    catch (system_error& e) {
        return GETATTR3res{exportStatus(e)};
    }
}

SETATTR3res NfsServer::setattr(const SETATTR3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::setattr("
                << formatFileHandle(args.object) << ", ...)";
    wcc_attr wcc;
    shared_ptr<File> obj;
    try {
        obj = importFileHandle(args.object);
        wcc = exportWcc(obj);
        obj->setattr(importAttr(args.new_attributes));
        return SETATTR3res{
            NFS3_OK,
            SETATTR3resok{
                wcc_data{
                    pre_op_attr(true, move(wcc)),
                    post_op_attr(true, exportAttr(obj))}}};
    }
    catch (system_error& e) {
        if (obj) {
            return SETATTR3res{
                exportStatus(e),
                SETATTR3resfail{
                    wcc_data{
                        pre_op_attr(true, move(wcc)),
                        post_op_attr(true, exportAttr(obj))}}};
        }
        else {
            return SETATTR3res{
                exportStatus(e),
                SETATTR3resfail{
                    wcc_data{pre_op_attr(false), post_op_attr(false)}}};
        }
    }
}

LOOKUP3res NfsServer::lookup(const LOOKUP3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::lookup("
                << formatFileHandle(args.what.dir)
                << ", \"" << args.what.name << "\")";
    shared_ptr<File> dir;
    try {
        dir = importFileHandle(args.what.dir);
        auto obj = dir->lookup(args.what.name);
        return LOOKUP3res{
            NFS3_OK,
            LOOKUP3resok{
                exportFileHandle(obj),
                post_op_attr(true, exportAttr(obj)),
                post_op_attr(true, exportAttr(dir))}};
    }
    catch (system_error& e) {
        if (dir) {
            return LOOKUP3res{
                exportStatus(e),
                LOOKUP3resfail{post_op_attr(true, exportAttr(dir))}};
        }
        else {
            return LOOKUP3res{
                exportStatus(e),
                LOOKUP3resfail{post_op_attr(false)}};
        }
    }
}

ACCESS3res NfsServer::access(const ACCESS3args& args)
{
    return ACCESS3res{
        NFS3ERR_NOTSUPP,
        ACCESS3resfail{post_op_attr(false)}};
}

READLINK3res NfsServer::readlink(const READLINK3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::readlink("
                << formatFileHandle(args.symlink) << ")";
    shared_ptr<File> obj;
    try {
        obj = importFileHandle(args.symlink);
        return READLINK3res{
            NFS3_OK,
            READLINK3resok{
                post_op_attr(true, exportAttr(obj)),
                obj->readlink()}};
    }
    catch (system_error& e) {
        if (obj) {
            return READLINK3res{
                exportStatus(e),
                READLINK3resfail{post_op_attr(true, exportAttr(obj))}};
        }
        else {
            return READLINK3res{
                exportStatus(e),
                READLINK3resfail{post_op_attr(false)}};
        }
    }
}

READ3res NfsServer::read(const READ3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::read("
                << formatFileHandle(args.file)
                << ", " << args.offset
                << ", " << args.count << ")";
    shared_ptr<File> obj;
    try {
        obj = importFileHandle(args.file);
        bool eof;
        auto data = obj->read(args.offset, args.count, eof);
        return READ3res{
            NFS3_OK,
            READ3resok{
                post_op_attr(true, exportAttr(obj)),
                count3(data->size()),
                eof,
                data}};
    }
    catch (system_error& e) {
        if (obj) {
            return READ3res{
                exportStatus(e),
                READ3resfail{post_op_attr(true, exportAttr(obj))}};
        }
        else {
            return READ3res{
                exportStatus(e),
                READ3resfail{post_op_attr(false)}};
        }
    }
}

WRITE3res NfsServer::write(const WRITE3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::write("
                << formatFileHandle(args.file)
                << ", " << args.offset, ", ...)";
    wcc_attr wcc;
    shared_ptr<File> obj;
    try {
        obj = importFileHandle(args.file);
        auto n = obj->write(args.offset, args.data);
        // XXX: args.Settable
        return WRITE3res{
            NFS3_OK,
            WRITE3resok{
                wcc_data{
                    pre_op_attr(true, move(wcc)),
                    post_op_attr(true, exportAttr(obj))},
                n,
                FILE_SYNC,  // XXX: bogus
                {}}};
    }
    catch (system_error& e) {
        if (obj) {
            return WRITE3res{
                exportStatus(e),
                WRITE3resfail{
                    wcc_data{
                        pre_op_attr(true, move(wcc)),
                        post_op_attr(true, exportAttr(obj))}}};
        }
        else {
            return WRITE3res{
                exportStatus(e),
                WRITE3resfail{
                    wcc_data{pre_op_attr(false), post_op_attr(false)}}};
        }
    }
}

CREATE3res NfsServer::create(const CREATE3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::create("
                << formatFileHandle(args.where.dir)
                << ", \"" << args.where.name << "\""
                << ", " << args.how.mode << ", ...)";
    wcc_attr wcc;
    shared_ptr<File> dir;
    try {
        dir = importFileHandle(args.where.dir);
        int flags = OpenFlags::RDWR | OpenFlags::CREATE;
        switch (args.how.mode) {
        case UNCHECKED:
            break;
        case GUARDED:
            flags |= OpenFlags::EXCLUSIVE;
            break;
        case EXCLUSIVE:
            throw system_error(EOPNOTSUPP, system_category());
        }
        auto obj = dir->open(
            args.where.name, flags, importAttr(args.how.obj_attributes()));
        return CREATE3res{
            NFS3_OK,
            CREATE3resok{
                post_op_fh3(true, exportFileHandle(obj)),
                post_op_attr(true, exportAttr(obj)),
                wcc_data{
                    pre_op_attr(true, move(wcc)),
                    post_op_attr(true, exportAttr(dir))}}};
    }
    catch (system_error& e) {
        if (dir) {
            return CREATE3res{
                exportStatus(e),
                CREATE3resfail{
                    wcc_data{
                        pre_op_attr(true, move(wcc)),
                        post_op_attr(true, exportAttr(dir))}}};
        }
        else {
            return CREATE3res{
                exportStatus(e),
                CREATE3resfail{
                    wcc_data{pre_op_attr(false), post_op_attr(false)}}};
        }
    }
}

MKDIR3res NfsServer::mkdir(const MKDIR3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::mkdir("
                << formatFileHandle(args.where.dir)
                << ", \"" << args.where.name << "\", ...)";
    wcc_attr wcc;
    shared_ptr<File> dir;
    try {
        dir = importFileHandle(args.where.dir);
        wcc = exportWcc(dir);
        auto obj = dir->mkdir(
            args.where.name, importAttr(args.attributes));
        return MKDIR3res{
            NFS3_OK,
            MKDIR3resok{
                post_op_fh3{true, exportFileHandle(obj)},
                post_op_attr(true, exportAttr(obj)),
                wcc_data{
                    pre_op_attr(true, move(wcc)),
                    post_op_attr(true, exportAttr(dir))}}};
    }
    catch (system_error& e) {
        if (dir) {
            return MKDIR3res{
                exportStatus(e),
                MKDIR3resfail{
                    wcc_data{
                        pre_op_attr(true, move(wcc)),
                        post_op_attr(true, exportAttr(dir))}}};
        }
        else {
            return MKDIR3res{
                exportStatus(e),
                MKDIR3resfail{
                    wcc_data{pre_op_attr(false), post_op_attr(false)}}};
        }
    }
}

SYMLINK3res NfsServer::symlink(const SYMLINK3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::symlink("
                << formatFileHandle(args.where.dir)
                << ", \"" << args.where.name << "\""
                << ", \"" << args.symlink.symlink_data << "\", ...)";
    wcc_attr wcc;
    shared_ptr<File> dir;
    try {
        dir = importFileHandle(args.where.dir);
        wcc = exportWcc(dir);
        auto obj = dir->symlink(
            args.where.name,
            args.symlink.symlink_data,
            importAttr(args.symlink.symlink_attributes));
        return SYMLINK3res{
            NFS3_OK,
            SYMLINK3resok{
                post_op_fh3{true, exportFileHandle(obj)},
                post_op_attr(true, exportAttr(obj)),
                wcc_data{
                    pre_op_attr(true, move(wcc)),
                    post_op_attr(true, exportAttr(dir))}}};
    }
    catch (system_error& e) {
        if (dir) {
            return SYMLINK3res{
                exportStatus(e),
                SYMLINK3resfail{
                    wcc_data{
                        pre_op_attr(true, move(wcc)),
                        post_op_attr(true, exportAttr(dir))}}};
        }
        else {
            return SYMLINK3res{
                exportStatus(e),
                SYMLINK3resfail{
                    wcc_data{pre_op_attr(false), post_op_attr(false)}}};
        }
    }
}

MKNOD3res NfsServer::mknod(const MKNOD3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::mknod("
                << formatFileHandle(args.where.dir)
                << ", \"" << args.where.name << "\""
                << ", " << args.what.type << ", ...)";
    wcc_attr wcc;
    shared_ptr<File> dir;
    try {
        dir = importFileHandle(args.where.dir);
        wcc = exportWcc(dir);
        if (args.what.type != NF3FIFO) {
            throw system_error(EINVAL, system_category());
        }
        auto obj = dir->mkfifo(
            args.where.name,
            importAttr(args.what.pipe_attributes()));
        return MKNOD3res{
            NFS3_OK,
            MKNOD3resok{
                post_op_fh3{true, exportFileHandle(obj)},
                post_op_attr(true, exportAttr(obj)),
                wcc_data{
                    pre_op_attr(true, move(wcc)),
                    post_op_attr(true, exportAttr(dir))}}};
    }
    catch (system_error& e) {
        if (dir) {
            return MKNOD3res{
                exportStatus(e),
                MKNOD3resfail{
                    wcc_data{
                        pre_op_attr(true, move(wcc)),
                        post_op_attr(true, exportAttr(dir))}}};
        }
        else {
            return MKNOD3res{
                exportStatus(e),
                MKNOD3resfail{
                    wcc_data{pre_op_attr(false), post_op_attr(false)}}};
        }
    }
}

REMOVE3res NfsServer::remove(const REMOVE3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::remove("
                << formatFileHandle(args.object.dir)
                << ", \"" << args.object.name << "\")";
    wcc_attr wcc;
    shared_ptr<File> dir;
    try {
        dir = importFileHandle(args.object.dir);
        wcc = exportWcc(dir);
        dir->remove(args.object.name);
        return REMOVE3res{
            NFS3_OK,
            REMOVE3resok{
                wcc_data{
                    pre_op_attr(true, move(wcc)),
                    post_op_attr(true, exportAttr(dir))}}};
    }
    catch (system_error& e) {
        if (dir) {
            return REMOVE3res{
                exportStatus(e),
                REMOVE3resfail{
                    wcc_data{
                        pre_op_attr(true, move(wcc)),
                        post_op_attr(true, exportAttr(dir))}}};
        }
        else {
            return REMOVE3res{
                exportStatus(e),
                REMOVE3resfail{
                    wcc_data{pre_op_attr(false), post_op_attr(false)}}};
        }
    }
}

RMDIR3res NfsServer::rmdir(const RMDIR3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::rmdir("
                << formatFileHandle(args.object.dir)
                << ", \"" << args.object.name << "\")";
    wcc_attr wcc;
    shared_ptr<File> dir;
    try {
        dir = importFileHandle(args.object.dir);
        wcc = exportWcc(dir);
        dir->rmdir(args.object.name);
        return RMDIR3res{
            NFS3_OK,
            RMDIR3resok{
                wcc_data{
                    pre_op_attr(true, move(wcc)),
                    post_op_attr(true, exportAttr(dir))}}};
    }
    catch (system_error& e) {
        if (dir) {
            return RMDIR3res{
                exportStatus(e),
                RMDIR3resfail{
                    wcc_data{
                        pre_op_attr(true, move(wcc)),
                        post_op_attr(true, exportAttr(dir))}}};
        }
        else {
            return RMDIR3res{
                exportStatus(e),
                RMDIR3resfail{
                    wcc_data{pre_op_attr(false), post_op_attr(false)}}};
        }
    }
}

RENAME3res NfsServer::rename(const RENAME3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::rename("
                << formatFileHandle(args.from.dir)
                << ", \"" << args.from.name << "\""
                << formatFileHandle(args.to.dir)
                << ", \"" << args.to.name << "\")";
    wcc_attr fromwcc, towcc;
    shared_ptr<File> fromdir, todir;
    try {
        fromdir = importFileHandle(args.from.dir);
        fromwcc = exportWcc(fromdir);
        todir = importFileHandle(args.to.dir);
        towcc = exportWcc(todir);
        todir->rename(args.to.name, fromdir, args.from.name);
        return RENAME3res{
            NFS3_OK,
            RENAME3resok{
                wcc_data{
                    pre_op_attr(true, move(fromwcc)),
                    post_op_attr(true, exportAttr(fromdir))},
                wcc_data{
                    pre_op_attr(true, move(towcc)),
                    post_op_attr(true, exportAttr(todir))}}};
    }
    catch (system_error& e) {
        if (fromdir && todir) {
            return RENAME3res{
                exportStatus(e),
                RENAME3resfail{
                    wcc_data{
                        pre_op_attr(true, move(fromwcc)),
                        post_op_attr(true, exportAttr(fromdir))},
                    wcc_data{
                        pre_op_attr(true, move(towcc)),
                        post_op_attr(true, exportAttr(todir))}}};
         }
         else {
            return RENAME3res{
                exportStatus(e),
                RENAME3resfail{
                    wcc_data{pre_op_attr(false), post_op_attr(false)},
                    wcc_data{pre_op_attr(false), post_op_attr(false)}}};
        }
    }
}

LINK3res NfsServer::link(const LINK3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::link("
                << formatFileHandle(args.file)
                << formatFileHandle(args.link.dir)
                << ", \"" << args.link.name << "\"";
    wcc_attr wcc;
    shared_ptr<File> obj, dir;
    try {
        obj = importFileHandle(args.file);
        dir = importFileHandle(args.link.dir);
        wcc = exportWcc(dir);
        dir->link(args.link.name, obj);
        return LINK3res{
            NFS3_OK,
            LINK3resok{
                post_op_attr(true, exportAttr(obj)),
                wcc_data{
                    pre_op_attr(true, move(wcc)),
                    post_op_attr(true, exportAttr(dir))}}};
    }
    catch (system_error& e) {
        if (obj && dir) {
            return LINK3res{
                exportStatus(e),
                LINK3resfail{
                    post_op_attr(true, exportAttr(obj)),
                    wcc_data{
                        pre_op_attr(true, move(wcc)),
                        post_op_attr(true, exportAttr(dir))}}};
        }
        else {
            return LINK3res{
                exportStatus(e),
                LINK3resfail{
                    post_op_attr(false),
                    wcc_data{pre_op_attr(false), post_op_attr(false)}}};
        }
    }
}

READDIR3res NfsServer::readdir(const READDIR3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::readdir("
                << formatFileHandle(args.dir)
                << ", " << args.cookie << ")";
    shared_ptr<File> dir;
    try {
        dir = importFileHandle(args.dir);
        // XXX: cookieverf
        READDIR3res res;
        res.set_status(NFS3_OK);
        res.resok().dir_attributes = post_op_attr(true, exportAttr(dir));
        res.resok().cookieverf = args.cookieverf;
        res.resok().reply.eof = true;
        auto replySize = XdrSizeof(res);
        unique_ptr<entry3>* entryp = &res.resok().reply.entries;
        for (auto iter = dir->readdir(args.cookie);
            iter->valid(); iter->next()) {
            auto entry = make_unique<entry3>();
            entry->fileid = iter->fileid();
            entry->name = iter->name();
            entry->cookie = iter->seek();
            auto entrySize = XdrSizeof(*entry.get());
            if (replySize + entrySize < args.count) {
                *entryp = move(entry);
                entryp = &(*entryp)->nextentry;
                replySize += entrySize;
            }
            else {
                res.resok().reply.eof = false;
                break;
            }
        }
        return res;
    }
    catch (system_error& e) {
        if (dir) {
            return READDIR3res{
                exportStatus(e),
                READDIR3resfail{post_op_attr(true, exportAttr(dir))}};
        }
        else {
            return READDIR3res{
                exportStatus(e),
                READDIR3resfail{post_op_attr(false)}};
        }
    }
}

READDIRPLUS3res NfsServer::readdirplus(const READDIRPLUS3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::readdirplus("
                << formatFileHandle(args.dir)
                << ", " << args.cookie << ")";
    shared_ptr<File> dir;
    try {
        dir = importFileHandle(args.dir);
        // XXX: cookieverf
        READDIRPLUS3res res;
        res.set_status(NFS3_OK);
        res.resok().dir_attributes = post_op_attr(true, exportAttr(dir));
        res.resok().cookieverf = args.cookieverf;
        res.resok().reply.eof = true;
        count3 replySize = XdrSizeof(res);
        count3 dirSize = 0;
        unique_ptr<entryplus3>* entryp = &res.resok().reply.entries;
        res.resok().reply.eof = true;
        for (auto iter = dir->readdir(args.cookie);
            iter->valid(); iter->next()) {
            auto entry = make_unique<entryplus3>();
            entry->fileid = iter->fileid();
            entry->name = iter->name();
            entry->cookie = iter->seek();
            auto file = iter->file();
            entry->name_attributes = post_op_attr(true, exportAttr(file));
            entry->name_handle = post_op_fh3(true, exportFileHandle(file));

            // Calculate the full entry size as well as the size of just
            // the directory information
            entry3 tmp{entry->fileid, entry->name, entry->cookie};
            auto dirEntrySize = XdrSizeof(tmp);
            auto entrySize = XdrSizeof(*entry);

            // Apparently some broken clients set dircount to zero
            if ((args.dircount == 0 || dirSize + dirEntrySize < args.dircount)
                && replySize + entrySize < args.maxcount) {
                *entryp = move(entry);
                entryp = &(*entryp)->nextentry;
                dirSize += dirEntrySize;
                replySize += entrySize;
            }
            else {
                res.resok().reply.eof = false;
                break;
            }
        }
        return res;
    }
    catch (system_error& e) {
        if (dir) {
            return READDIRPLUS3res{
                exportStatus(e),
                READDIRPLUS3resfail{post_op_attr(true, exportAttr(dir))}};
        }
        else {
            return READDIRPLUS3res{
                exportStatus(e),
                READDIRPLUS3resfail{post_op_attr(false)}};
        }
    }
}

FSSTAT3res NfsServer::fsstat(const FSSTAT3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::fsstat("
                << formatFileHandle(args.fsroot) << ")";
    shared_ptr<File> obj;
    try {
        obj = importFileHandle(args.fsroot);
        auto stat = obj->fsstat();
        return FSSTAT3res{
            NFS3_OK,
            FSSTAT3resok{
                post_op_attr(true, exportAttr(obj)),
                stat->tbytes(),
                stat->abytes(),
                stat->fbytes(),
                stat->tfiles(),
                stat->afiles(),
                stat->ffiles(),
                0}};
    }
    catch (system_error& e) {
        if (obj) {
            return FSSTAT3res{
                exportStatus(e),
                FSSTAT3resfail{post_op_attr(true, exportAttr(obj))}};
        }
        else {
            return FSSTAT3res{
                exportStatus(e),
                FSSTAT3resfail{post_op_attr(false)}};
        }
    }
}

FSINFO3res NfsServer::fsinfo(const FSINFO3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::fsinfo("
                << formatFileHandle(args.fsroot) << ")";
    shared_ptr<File> obj;
    try {
        obj = importFileHandle(args.fsroot);
        auto stat = obj->fsstat();
        std::uint32_t sz = FLAGS_iosize;
        std::uint32_t properties =
            FSF3_LINK + FSF3_SYMLINK + FSF3_HOMOGENEOUS + FSF3_CANSETTIME;
        return FSINFO3res{
            NFS3_OK,
            FSINFO3resok{
                post_op_attr(true, exportAttr(obj)),
                sz,          // rtmax
                sz,           // rtpref
                512,            // rtmult
                sz,          // wtmax
                sz,           // wtpref
                512,            // wtmult
                sz,           // dtpref
                0xffffffffffffffffull, // maxfilesize
                {0, 1},         // timedelta
                properties}};
    }
    catch (system_error& e) {
        if (obj) {
            return FSINFO3res{
                exportStatus(e),
                FSINFO3resfail{post_op_attr(true, exportAttr(obj))}};
        }
        else {
            return FSINFO3res{
                exportStatus(e),
                FSINFO3resfail{post_op_attr(false)}};
        }
    }
}

PATHCONF3res NfsServer::pathconf(const PATHCONF3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::pathconf("
                << formatFileHandle(args.object) << ")";
    shared_ptr<File> obj;
    try {
        obj = importFileHandle(args.object);
        auto stat = obj->fsstat();
        return PATHCONF3res{
            NFS3_OK,
            PATHCONF3resok{
                post_op_attr(true, exportAttr(obj)),
                uint32_t(stat->linkMax()),
                uint32_t(stat->nameMax()),
                true,
                true,
                true,
                true}};
    }
    catch (system_error& e) {
        if (obj) {
            return PATHCONF3res{
                exportStatus(e),
                PATHCONF3resfail{post_op_attr(true, exportAttr(obj))}};
        }
        else {
            return PATHCONF3res{
                exportStatus(e),
                PATHCONF3resfail{post_op_attr(false)}};
        }
    }
}

COMMIT3res NfsServer::commit(const COMMIT3args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::commit("
                << formatFileHandle(args.file)
                << ", " << args.offset
                << ", " << args.count << ")";
    wcc_attr wcc;
    shared_ptr<File> obj;
    try {
        obj = importFileHandle(args.file);
        wcc = exportWcc(obj);
        // XXX: offset, count
        obj->commit();
        // XXX: writeverf
        return COMMIT3res{
            NFS3_OK,
            COMMIT3resok{
                wcc_data{
                    pre_op_attr(true, move(wcc)),
                    post_op_attr(true, exportAttr(obj))},
                {}}};
    }
    catch (system_error& e) {
        if (obj) {
            return COMMIT3res{
                NFS3ERR_NOTSUPP,
                COMMIT3resfail{
                    wcc_data{
                        pre_op_attr(true, move(wcc)),
                        post_op_attr(true, exportAttr(obj))}}};
        }
        else {
            return COMMIT3res{
                NFS3ERR_NOTSUPP,
                COMMIT3resfail{
                    wcc_data{pre_op_attr(false), post_op_attr(false)}}};
        }
    }
}
