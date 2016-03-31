#include <cassert>
#include <system_error>

#include "nfs4fs.h"

using namespace filesys;
using namespace filesys::nfs4;
using namespace std;

NfsFile::NfsFile(
    std::shared_ptr<NfsFilesystem> fs, nfs_fh4&& fh, fattr4&& attr)
    : fs_(fs),
      fh_(move(fh)),
      attrTime_(fs->clock()->now()),
      attr_(move(attr)),
      writeverf_({{0,0,0,0, 0,0,0,0}})
{
}

shared_ptr<Filesystem> NfsFile::fs()
{
    return fs_.lock();
}

void
NfsFile::handle(FileHandle& fh)
{
    throw system_error(EOPNOTSUPP, system_category());
}

bool NfsFile::access(const Credential& cred, int accmode)
{
    uint32_t flags = 0;
    if (accmode & AccessFlags::READ)
        flags |= ACCESS4_READ;
    if (accmode & AccessFlags::WRITE)
        flags |= ACCESS4_MODIFY;
    if (accmode & AccessFlags::EXECUTE) {
        if (attr_.type_ == NF4DIR)
            flags |= ACCESS4_LOOKUP;
        else
            flags |= ACCESS4_EXECUTE;
    }
    auto fs = fs_.lock();
    bool res;
    fs->compound(
        [this, flags](auto& enc) {
            enc.putfh(fh_);
            enc.access(flags);
        },
        [&res, flags](auto& dec) {
            dec.putfh();
            res = dec.access().access == flags;
        });
    return res;
}

shared_ptr<Getattr> NfsFile::getattr()
{
    auto deleg = delegation_.lock();
    auto fs = fs_.lock();
    if (!deleg) {
        auto now = fs->clock()->now();
        if (now - attrTime_ > ATTR_TIMEOUT) {
            fs->compound(
                [this](auto& enc) {
                    bitmap4 wanted;
                    setSupportedAttrs(wanted);
                    enc.putfh(fh_);
                    enc.getattr(wanted);
                },
                [this](auto& dec) {
                    dec.putfh();
                    update(move(dec.getattr().obj_attributes));
                });
        }
    }
    return make_shared<NfsGetattr>(attr_, fs->idmapper());
}

void NfsFile::setattr(const Credential&, function<void(Setattr*)> cb)
{
    auto fs = fs_.lock();

    NfsSetattr sattr;
    cb(&sattr);
    fattr4 attr;
    sattr.encode(attr);
    fs->compound(
        [this, &attr](auto& enc) {
            bitmap4 wanted;
            setSupportedAttrs(wanted);
            enc.putfh(fh_);
            enc.setattr(STATEID_ANON, attr);
            enc.getattr(wanted);
        },
        [this](auto& dec) {
            dec.putfh();
            dec.setattr();
            update(move(dec.getattr().obj_attributes));
        });
}

shared_ptr<File> NfsFile::lookup(const Credential&, const string& name)
{
    auto fs = fs_.lock();

    if (attr_.type_ != NF4DIR)
        throw system_error(ENOTDIR, system_category());

    // Handle '.' and '..' specially
    if (name == ".")
        return shared_from_this();

    nfs_fh4 fh;
    fattr4 attr;
    fs->compound(
        [this, &name](auto& enc) {
            bitmap4 wanted;
            setSupportedAttrs(wanted);
            enc.putfh(fh_);
            enc.getattr(wanted);
            if (name == "..")
                enc.lookupp();
            else
                enc.lookup(toUtf8string(name));
            enc.getattr(wanted);
            enc.getfh();
        },
        [this, &name, &fh, &attr](auto& dec) {
            dec.putfh();
            update(move(dec.getattr().obj_attributes));
            if (name == "..")
                dec.lookupp();
            else
                dec.lookup();
            attr = move(dec.getattr().obj_attributes);
            fh = move(dec.getfh().object);
        });

    return fs->find(move(fh), move(attr));
}

shared_ptr<OpenFile> NfsFile::open(
    const Credential& cred, const string& name, int flags,
    function<void(Setattr*)> cb)
{
    auto fs = fs_.lock();

    // If we are opening non-exclusively, first check to see if it
    // already exists since we might already have a delegation
    if ((flags & OpenFlags::EXCLUSIVE) == 0) {
        try {
            nfs_fh4 fh;
            fattr4 attr;
            fs->compound(
                [this, &name](auto& enc) {
                    bitmap4 wanted;
                    setSupportedAttrs(wanted);
                    enc.putfh(fh_);
                    enc.getattr(wanted);
                    enc.lookup(toUtf8string(name));
                    enc.getattr(wanted);
                    enc.getfh();
                },
                [this, &name, &fh, &attr](auto& dec) {
                    dec.putfh();
                    update(move(dec.getattr().obj_attributes));
                    dec.lookup();
                    attr = move(dec.getattr().obj_attributes);
                    fh = move(dec.getfh().object);
                });
            auto file = fs->find(move(fh), move(attr));
            if (file->open_.lock()) {
                // If we don't already have the file open, use the full
                // logic below so that we handle truncate correctly.
                return file->open(cred, flags);
            }
        }
        catch (system_error& e) {
            if (e.code().value() == ENOENT) {
            }
            else {
                throw;
            }
        }
    }

    // We open everything with the same owner
    open_owner4 oo{ fs->clientid(), { 1, 0, 0, 0 } };
    nfs_fh4 fh;
    fattr4 attr;
    OPEN4resok res;
    fs->compound(
        [this, flags, &name, &oo, cb](auto& enc) {
            bitmap4 wanted;
            setSupportedAttrs(wanted);
            enc.putfh(fh_);
            enc.getattr(wanted);
            int access = 0;
            int deny = 0;
            if (flags & OpenFlags::READ) {
                access |= OPEN4_SHARE_ACCESS_READ;
            }
            if (flags & OpenFlags::WRITE) {
                access |= OPEN4_SHARE_ACCESS_WRITE;
            }
            if (flags & OpenFlags::SHLOCK) {
                deny |= OPEN4_SHARE_DENY_WRITE;
            }
            if (flags & OpenFlags::EXLOCK) {
                deny |= OPEN4_SHARE_DENY_BOTH;
            }
            openflag4 openflag;
            if (flags & OpenFlags::CREATE) {
                NfsSetattr sattr;
                cb(&sattr);
                if (flags & OpenFlags::TRUNCATE) {
                    sattr.setSize(0);
                }
                fattr4 attr;
                sattr.encode(attr);
                if (flags & OpenFlags::EXCLUSIVE) {
                    verifier4 verf;
                    *reinterpret_cast<uint64_t*>(verf.data()) =
                        chrono::system_clock::now().time_since_epoch().count();
                    openflag = openflag4(
                        OPEN4_CREATE,
                        createhow4(EXCLUSIVE4_1, { verf, attr }));
                }
                else {
                    openflag = openflag4(
                        OPEN4_CREATE,
                        createhow4(UNCHECKED4, move(attr)));
                }
            }
            else {
                openflag = openflag4(OPEN4_NOCREATE);
            }
            enc.open(
                0, access, deny, oo, move(openflag),
                open_claim4(CLAIM_NULL, toUtf8string(name)));
            enc.getattr(wanted);
            enc.getfh();
        },
        [this, &res, &attr, &fh](auto& dec) {
            dec.putfh();
            update(move(dec.getattr().obj_attributes));
            res = dec.open();
            attr = move(dec.getattr().obj_attributes);
            fh = move(dec.getfh().object);
        });
    auto f = fs->find(move(fh), move(attr));
    auto of = make_shared<NfsOpenFile>(f, res.stateid, flags);
    f->open_ = of;
    f->delegation_ = fs->addDelegation(f, of, move(res.delegation));
    return of;
}

std::shared_ptr<OpenFile> NfsFile::open(const Credential& cred, int flags)
{
    std::unique_lock<std::mutex> lock(mutex_);
    auto fs = fs_.lock();

    if (flags & OpenFlags::EXCLUSIVE)
        throw system_error(EINVAL, system_category());
    flags &= (OpenFlags::RDWR | OpenFlags::TRUNCATE);

    // Check for an existing open, upgrading if necessary
    auto of = open_.lock();
    if (of) {
        // If the requested flags are a subset of the existing open,
        // we can use it, otherwise we need to upgrade it
        bool truncate = (flags & OpenFlags::TRUNCATE) != 0;
        flags &= OpenFlags::RDWR;
        if ((of->flags() & flags) == flags) {
            VLOG(1) << "using existing OpenFile, stateid: "
                    << of->stateid();
            if (truncate) {
                auto deleg = delegation_.lock();
                if (deleg && deleg->isWrite()) {
                    cache_.clear();
                    if (!modified_) {
                        attr_.change_++;
                        lastChange_ = attr_.change_;
                    }
                    attr_.time_modify_ = toNfsTime(fs->clock()->now());
                    attr_.size_ = 0;
                }
                else {
                    lock.unlock();
                    setattr(cred, [](auto attr) { attr->setSize(0); });
                }
            }
            return of;
        }
    }
    lock.unlock();

    // We open everything with the same owner
    open_owner4 oo{ fs->clientid(), { 1, 0, 0, 0 } };
    OPEN4resok res;
    fs->compound(
        [this, flags, &oo](auto& enc) {
            enc.putfh(fh_);
            int access = 0;
            int deny = 0;
            if (flags & OpenFlags::READ) {
                access |= OPEN4_SHARE_ACCESS_READ;
                //access |= OPEN4_SHARE_ACCESS_WANT_READ_DELEG;
            }
            if (flags & OpenFlags::WRITE) {
                access |= OPEN4_SHARE_ACCESS_WRITE;
                //access |= OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG;
            }
            if (flags & OpenFlags::SHLOCK) {
                deny |= OPEN4_SHARE_DENY_WRITE;
            }
            if (flags & OpenFlags::EXLOCK) {
                deny |= OPEN4_SHARE_DENY_BOTH;
            }
            enc.open(
                0, access, deny, oo,
                openflag4(OPEN4_NOCREATE), open_claim4(CLAIM_FH));
        },
        [this, &res](auto& dec) {
            dec.putfh();
            res = dec.open();
        });

    if (of) {
        of->setStateid(res.stateid);
        of->setFlags(flags);
        return of;
    }
    else {
        of = make_shared<NfsOpenFile>(shared_from_this(), res.stateid, flags);
    }
    open_ = of;
    delegation_ = fs->addDelegation(
        shared_from_this(), of, move(res.delegation));
    return of;
}

string NfsFile::readlink(const Credential&)
{
    auto fs = fs_.lock();

    linktext4 data;
    fs->compound(
        [this](auto& enc) {
            bitmap4 wanted;
            setSupportedAttrs(wanted);
            enc.putfh(fh_);
            enc.readlink();
            enc.getattr(wanted);
        },
        [this, &data](auto& dec) {
            dec.putfh();
            data = dec.readlink().link;
            update(move(dec.getattr().obj_attributes));
        });
    return toString(data);
}

shared_ptr<File> NfsFile::mkdir(
    const Credential&, const string& name, function<void(Setattr*)> cb)
{
    return create(createtype4(NF4DIR), toUtf8string(name), cb);
}

shared_ptr<File> NfsFile::symlink(
    const Credential&, const string& name, const string& data,
    function<void(Setattr*)> cb)
{
    return create(
        createtype4(NF4LNK, toUtf8string(data)), toUtf8string(name), cb);
}

std::shared_ptr<File> NfsFile::mkfifo(
    const Credential&, const std::string& name,
    std::function<void(Setattr*)> cb)
{
    return create(createtype4(NF4FIFO), toUtf8string(name), cb);
}

void NfsFile::remove(const Credential&, const string& name)
{
    auto fs = fs_.lock();
    fs->compound(
        [this, &name](auto& enc) {
            bitmap4 wanted;
            setSupportedAttrs(wanted);

            // Check that the object being removed is not a directory
            fattr4 check;
            set(check.attrmask, FATTR4_TYPE);
            check.attr_vals.resize(sizeof(oncrpc::XdrWord));
            *reinterpret_cast<oncrpc::XdrWord*>
                (check.attr_vals.data()) = NF4DIR;

            auto uname = toUtf8string(name);

            enc.putfh(fh_);
            enc.savefh();
            enc.lookup(uname);
            enc.nverify(check);
            enc.restorefh();
            enc.remove(uname);
            enc.getattr(wanted);
        },
        [this](auto& dec) {
            dec.putfh();
            dec.savefh();
            dec.lookup();
            if (dec.nverify() == NFS4ERR_SAME)
                throw system_error(EISDIR, system_category());
            dec.restorefh();
            dec.remove();
            update(move(dec.getattr().obj_attributes));
        });
}

void NfsFile::rmdir(const Credential&, const string& name)
{
    auto fs = fs_.lock();
    fs->compound(
        [this, &name](auto& enc) {
            bitmap4 wanted;
            setSupportedAttrs(wanted);

            // Check that the object being removed is a directory
            fattr4 check;
            set(check.attrmask, FATTR4_TYPE);
            check.attr_vals.resize(sizeof(oncrpc::XdrWord));
            *reinterpret_cast<oncrpc::XdrWord*>
                (check.attr_vals.data()) = NF4DIR;

            auto uname = toUtf8string(name);

            enc.putfh(fh_);
            enc.savefh();
            enc.lookup(uname);
            enc.verify(check);
            enc.restorefh();
            enc.remove(uname);
            enc.getattr(wanted);
        },
        [this](auto& dec) {
            dec.putfh();
            dec.savefh();
            dec.lookup();
            if (dec.verify() == NFS4ERR_NOT_SAME)
                throw system_error(ENOTDIR, system_category());
            dec.restorefh();
            dec.remove();
            update(move(dec.getattr().obj_attributes));
        });
}

void NfsFile::rename(
    const Credential&, const string& toName,
    shared_ptr<File> fromDir, const string& fromName)
{
    auto fs = fs_.lock();
    auto from = dynamic_cast<NfsFile*>(fromDir.get());
    fs->compound(
        [this, from, &fromName, &toName](auto& enc) {
            bitmap4 wanted;
            setSupportedAttrs(wanted);
            enc.putfh(from->fh_);
            enc.savefh();
            enc.putfh(fh_);
            enc.rename(toUtf8string(fromName), toUtf8string(toName));
            enc.getattr(wanted);
            enc.restorefh();
            enc.getattr(wanted);
        },
        [this, from](auto& dec) {
            dec.putfh();
            dec.savefh();
            dec.putfh();
            dec.rename();
            update(move(dec.getattr().obj_attributes));
            dec.restorefh();
            from->update(move(dec.getattr().obj_attributes));
        });
}

void NfsFile::link(
    const Credential&, const std::string& name, std::shared_ptr<File> file)
{
    auto fs = fs_.lock();
    auto from = dynamic_cast<NfsFile*>(file.get());
    fs->compound(
        [this, from, &name](auto& enc) {
            bitmap4 wanted;
            setSupportedAttrs(wanted);
            enc.putfh(from->fh_);
            enc.savefh();
            enc.putfh(fh_);
            enc.link(toUtf8string(name));
            enc.getattr(wanted);
        },
        [this](auto& dec) {
            dec.putfh();
            dec.savefh();
            dec.putfh();
            dec.link();
            update(move(dec.getattr().obj_attributes));
        });
}

shared_ptr<DirectoryIterator> NfsFile::readdir(const Credential&, uint64_t seek)
{
    return make_shared<NfsDirectoryIterator>(shared_from_this(), seek);
}

std::shared_ptr<Fsattr> NfsFile::fsstat(const Credential&)
{
    auto res = make_shared<NfsFsattr>();
    auto fs = fs_.lock();
    fs->compound(
        [this](auto& enc) {
            bitmap4 wanted;
            set(wanted, FATTR4_FILES_AVAIL);
            set(wanted, FATTR4_FILES_FREE);
            set(wanted, FATTR4_FILES_TOTAL);
            set(wanted, FATTR4_SPACE_AVAIL);
            set(wanted, FATTR4_SPACE_FREE);
            set(wanted, FATTR4_SPACE_TOTAL);
            enc.putfh(fh_);
            enc.getattr(wanted);
        },
        [this, res](auto& dec) {
            dec.putfh();
            res->attr_.decode(dec.getattr().obj_attributes);
        });
    return res;
}

std::shared_ptr<NfsFile> NfsFile::lookupp()
{
    nfs_fh4 fh;
    fattr4 attr;
    auto fs = fs_.lock();
    fs->compound(
        [this](auto& enc) {
            bitmap4 wanted;
            setSupportedAttrs(wanted);
            enc.putfh(fh_);
            enc.lookupp();
            enc.getattr(wanted);
            enc.getfh();
        },
        [&fh, &attr](auto& dec) {
            dec.putfh();
            dec.lookupp();
            attr = move(dec.getattr().obj_attributes);
            fh = move(dec.getfh().object);
        });
    return fs->find(move(fh), move(attr));
}

shared_ptr<File> NfsFile::create(
    const createtype4& objtype, const utf8string& objname,
    function<void(Setattr*)> cb)
{
    auto fs = fs_.lock();

    NfsSetattr sattr;
    cb(&sattr);
    nfs_fh4 fh;
    fattr4 attr;
    sattr.encode(attr);
    fs->compound(
        [this, &objtype, &objname, &attr](auto& enc) {
            bitmap4 wanted;
            setSupportedAttrs(wanted);
            enc.putfh(fh_);
            enc.create(objtype, objname, attr);
            enc.getattr(wanted);
            enc.getfh();
            enc.putfh(fh_);
            enc.getattr(wanted);
        },
        [this, &attr, &fh](auto& dec) {
            dec.putfh();
            dec.create();
            attr = move(dec.getattr().obj_attributes);
            fh = move(dec.getfh().object);
            dec.putfh();
            update(move(dec.getattr().obj_attributes));
        });

    return fs->find(move(fh), move(attr));
}

void NfsFile::clearDelegation()
{
    auto deleg = delegation_.lock();
    if (deleg) {
        fs_.lock()->clearDelegation(deleg->stateid());
        delegation_.reset();
        modified_ = false;
    }
}

std::shared_ptr<Buffer> NfsFile::read(
    const stateid4& stateid, std::uint64_t offset, std::uint32_t count,
    bool& eof)
{
    auto fs = fs_.lock();
    std::unique_lock<std::mutex> lock(mutex_);
    auto buf = cache_.get(offset, count);
    if (buf) {
        VLOG(1) << "fileid: " << attr_.fileid_
                << ": read cache hit, offset: " << offset;
        eof = offset + buf->size() == attr_.size_;
        auto now = toNfsTime(fs->clock()->now());
        attr_.time_access_ = now;
        // XXX: avoid the rpc if we have a delegation?
        fs_.lock()->compound(
            [this, now](auto& enc) {
                NfsAttr xattr;
                set(xattr.attrmask_, FATTR4_TIME_ACCESS_SET);
                xattr.time_access_set_ = SET_TO_CLIENT_TIME4;
                xattr.time_access_ = now;
                fattr4 attr;
                xattr.encode(attr);
                enc.putfh(fh_);
                enc.setattr(STATEID_ANON, attr);
            },
            [this](auto dec) {
                dec.putfh();
                dec.setattr();
            });
        return buf;
    }
    VLOG(1) << "fileid: " << attr_.fileid_
            << ": read cache miss, offset: " << offset;
    lock.unlock();

    if (count > fs->fsinfo().maxread)
        count = fs->fsinfo().maxread;

    READ4resok res;
    fs->compound(
        [this, &stateid, offset, count](auto& enc) {
            bitmap4 wanted;
            setSupportedAttrs(wanted);
            enc.putfh(fh_);
            enc.read(stateid, offset, count);
            enc.getattr(wanted);
        },
        [this, &res](auto& dec) {
            dec.putfh();
            res = dec.read();
            update(move(dec.getattr().obj_attributes));
        });
    eof = res.eof;
    lock.lock();
    cache_.add(detail::DataCache::STABLE, offset, res.data);
    return move(res.data);
}

std::uint32_t NfsFile::write(
    const stateid4& stateid, std::uint64_t offset,
    std::shared_ptr<Buffer> data)
{
    auto fs = fs_.lock();
    std::unique_lock<std::mutex> lock(mutex_);
    auto deleg = delegation_.lock();
    if (deleg && deleg->isWrite()) {
        // If we have a write delegation, cache locally
        if (!modified_) {
            modified_ = true;
            attr_.change_++;
            lastChange_ = attr_.change_;
        }
        if (offset + data->size() > attr_.size_)
            attr_.size_ = offset + data->size();
        attr_.time_modify_ = toNfsTime(fs->clock()->now());
        cache_.add(detail::DataCache::DIRTY, offset, data);
        return data->size();
    }
    lock.unlock();

    if (data->size() > fs->fsinfo().maxwrite)
        data = make_shared<Buffer>(data, 0, fs->fsinfo().maxwrite);
    WRITE4resok res;
    fs->compound(
        [this, &stateid, offset, data](auto& enc) {
            bitmap4 wanted;
            setSupportedAttrs(wanted);
            enc.putfh(fh_);
            enc.write(stateid, offset, UNSTABLE4, data);
            enc.getattr(wanted);
        },
        [this, &res](auto& dec) {
            dec.putfh();
            res = dec.write();
            // Note: we don't just call update here since we are
            // expecting the value of change to be different and we
            // don't want to clear the cache
            attr_.decode(move(dec.getattr().obj_attributes));
            lastChange_ = attr_.change_;
        });

    lock.lock();

    if (haveWriteverf_) {
        if (res.writeverf != writeverf_) {
            // We need to re-write any cached blocks which are not marked
            // STABLE. Set the new writeverf first to avoid recursion.
            writeverf_ = res.writeverf;
            if (cache_.blockCount() > 0) {
                VLOG(1) << "fileid: " << attr_.fileid_
                        << ": write verifier changed, re-writing unstable data";
                cache_.apply(
                    [this](auto& state, auto start, auto end, auto data) {
                        if (state == detail::DataCache::UNSTABLE) {
                            state = detail::DataCache::DIRTY;
                        }
                    });
                lock.unlock();
                flush(stateid, false);
            }
        }
    }
    else {
        haveWriteverf_ = true;
        writeverf_ = res.writeverf;
    }

    detail::DataCache::State state;
    if (res.committed == UNSTABLE4)
        state = detail::DataCache::UNSTABLE;
    else
        state = detail::DataCache::STABLE;
    if (res.count == data->size())
        cache_.add(state, offset, data);
    else
        cache_.add(state, offset, make_shared<Buffer>(data, 0, res.count));

    return res.count;
}

void NfsFile::flush(const stateid4& stateid, bool commit)
{
    std::unique_lock<std::mutex> lock(mutex_);

    // First push any locally cached data to the server
retry:
    vector<pair<uint64_t, shared_ptr<Buffer>>> toWrite;
    cache_.apply(
        [this, &toWrite](auto state, auto start, auto end, auto data) {
            if (state == detail::DataCache::DIRTY) {
                toWrite.emplace_back(start, data);
            }
        });

    lock.unlock();
    for (auto& w: toWrite)
        write(stateid, w.first, w.second);
    toWrite.clear();

    if (commit) {
        uint64_t minStart = numeric_limits<uint64_t>::max();
        uint64_t maxEnd = numeric_limits<uint64_t>::min();

        lock.lock();
        cache_.apply(
            [&minStart, &maxEnd](auto state, auto start, auto end, auto data) {
                if (state == detail::DataCache::UNSTABLE) {
                    if (start < minStart)
                        minStart = start;
                    if (end > maxEnd)
                        maxEnd = end;
                }
            });
        lock.unlock();

        if (maxEnd > minStart) {
            VLOG(1) << "fileid: " << attr_.fileid_
                    << ": committing [" << minStart << "..." << maxEnd << ")";
            auto fs = fs_.lock();
            verifier4 writeverf;
            fs->compound(
                [this, minStart, maxEnd](auto& enc) {
                    enc.putfh(fh_);
                    enc.commit(minStart, maxEnd - minStart);
                },
                [&writeverf](auto& dec) {
                    dec.putfh();
                    writeverf = dec.commit().writeverf;
                });
            lock.lock();
            if (writeverf != writeverf_) {
                VLOG(1) << "fileid: " << attr_.fileid_
                        << ": write verifier changed, re-writing unstable data";
                writeverf_ = writeverf;
                cache_.apply(
                    [](auto& state, auto, auto, auto) {
                        if (state == detail::DataCache::UNSTABLE)
                            state = detail::DataCache::DIRTY;
                    });
                goto retry;
            }
            else {
                cache_.apply(
                    [](auto& state, auto, auto, auto) {
                        if (state == detail::DataCache::UNSTABLE)
                            state = detail::DataCache::STABLE;
                    });
            }
        }
    }
}

void NfsFile::update(fattr4&& attr)
{
    std::unique_lock<std::mutex> lock(mutex_);

    auto deleg = delegation_.lock();
    if (deleg && deleg->isWrite())
        return;
    attr_.decode(attr);
    if (lastChange_ < attr_.change_) {
        if (cache_.blockCount()) {
            VLOG(1) << "fileid: " << attr_.fileid_
                    << ": file changed, flushing cache";
            cache_.clear();
        }
        lastChange_ = attr_.change_;
    }
}

void NfsFile::recover()
{
    auto of = open_.lock();
    if (of) {
        auto fs = fs_.lock();

        // We open everything with the same owner
        open_owner4 oo{ fs->clientid(), { 1, 0, 0, 0 } };
        OPEN4resok res;
        fs->compound(
            [this, of, &oo](auto& enc) {
                enc.putfh(fh_);
                int access = 0;
                int deny = 0;
                if (of->flags() & OpenFlags::READ) {
                    access |= OPEN4_SHARE_ACCESS_READ;
                    //access |= OPEN4_SHARE_ACCESS_WANT_READ_DELEG;
                }
                if (of->flags() & OpenFlags::WRITE) {
                    access |= OPEN4_SHARE_ACCESS_WRITE;
                    //access |= OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG;
                }
                if (of->flags() & OpenFlags::SHLOCK) {
                    deny |= OPEN4_SHARE_DENY_WRITE;
                }
                if (of->flags() & OpenFlags::EXLOCK) {
                    deny |= OPEN4_SHARE_DENY_BOTH;
                }
                enc.open(
                    0, access, deny, oo,
                    openflag4(OPEN4_NOCREATE),
                    open_claim4(CLAIM_PREVIOUS, OPEN_DELEGATE_NONE));
            },
            [this, of, &res](auto& dec) {
                dec.putfh();
                try {
                    res = dec.open();
                }
                catch (nfsstat4 stat) {
                    if (stat == NFS4ERR_NO_GRACE) {
                        // It looks like the server did not restart -
                        // this recovery must have been prompted by a
                        // network partition. There isn't much we can
                        // do here safely - mark the object as dead.
                        of->setDead();
                        open_.reset();
                        return;
                    }
                }
            });
        of->setStateid(res.stateid);
    }
}

void NfsFile::testState()
{
    auto of = open_.lock();
    if (of) {
        auto fs = fs_.lock();
        bool bad = false;
        fs->compound(
            [of](auto& enc) {
                enc.test_stateid(vector<stateid4>{of->stateid()});
            },
            [of, &bad](auto& dec) {
                auto res = dec.test_stateid();
                if (res.tsr_status_codes[0] != NFS4_OK)
                    bad = true;
            });
        if (bad) {
            fs->compound(
                [of](auto& enc) {
                    enc.free_stateid(of->stateid());
                },
                [of, bad](auto& dec) {
                    dec.free_stateid();
                });
            of->setDead();
            open_.reset();
        }
    }
}

NfsOpenFile::~NfsOpenFile()
{
    if (!dead_) {
        file_->flush(stateid_, true);
        auto fs = file_->nfs();
        fs->compound(
            [this](auto& enc) {
                enc.putfh(file_->fh());
                enc.close(0, stateid_);
            },
            [this](auto& dec) {
                dec.putfh();
                try {
                    dec.close();
                }
                catch (nfsstat4 stat) {
                    // Ignore NFS4ERR_BAD_STATEID here. This can
                    // happen if the server restarts or expires our
                    // client. We can't easily recover since the
                    // NfsFile's weak_ptr is already invalid.
                    //
                    // XXX we could mitigate by moving the stateid_ to
                    // NfsFile which would be ok since we only allow
                    // one NfsOpenFile instance per NfsFile instance.
                    if (stat != NFS4ERR_BAD_STATEID)
                        throw;
                }
            });
    }
    stateid_ = STATEID_INVALID;
}

shared_ptr<Buffer> NfsOpenFile::read(
    uint64_t offset, uint32_t count, bool& eof)
{
    if (dead_)
        throw system_error(EBADF, system_category());
    return file_->read(stateid_, offset, count, eof);
}

uint32_t NfsOpenFile::write(uint64_t offset, shared_ptr<Buffer> data)
{
    if (dead_)
        throw system_error(EBADF, system_category());
    return file_->write(stateid_, offset, data);
}

void NfsOpenFile::flush()
{
    if (dead_)
        throw system_error(EBADF, system_category());
    file_->flush(stateid_, true);
}