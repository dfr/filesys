// -*- c++ -*-
#pragma once

#include <iomanip>
#include <string>
#include <map>
#include <vector>

#include <fs++/datacache.h>
#include <fs++/filesys.h>
#include <fs++/lrucache.h>
#include <rpc++/xdr.h>
#include <glog/logging.h>

#include "nfs4proto.h"
#include "nfs4util.h"
#include "nfs4compound.h"
#include "nfs4attr.h"
#include "nfs4cb.h"
#include "nfs4idmap.h"

namespace filesys {
namespace nfs4 {

constexpr detail::Clock::duration ATTR_TIMEOUT = std::chrono::seconds(5);

class NfsDelegation;
class NfsFilesystem;
class NfsOpenFile;

class NfsGetattr: public Getattr
{
public:
    NfsGetattr(const NfsAttr& attr, std::shared_ptr<IIdMapper> idmapper)
        : attr_(attr),
          idmapper_(idmapper)
    {
    }

    // Getattr overrides
    FileType type() const override;
    int mode() const override;
    int nlink() const override;
    int uid() const override;
    int gid() const override;
    std::uint64_t size() const override;
    std::uint64_t used() const override;
    FileId fileid() const override;
    std::chrono::system_clock::time_point mtime() const override;
    std::chrono::system_clock::time_point atime() const override;
    std::chrono::system_clock::time_point ctime() const override;
    std::chrono::system_clock::time_point birthtime() const override;
    std::uint64_t change() const override;
    std::uint64_t createverf() const override;
private:
    const NfsAttr& attr_;
    std::shared_ptr<IIdMapper> idmapper_;
};

class NfsSetattr: public Setattr, public NfsAttr
{
public:
    // Setattr overrides
    void setMode(int mode) override;
    void setUid(int uid) override;
    void setGid(int gid) override;
    void setSize(std::uint64_t size) override;
    void setMtime(std::chrono::system_clock::time_point mtime) override;
    void setAtime(std::chrono::system_clock::time_point atime) override;
    void setChange(std::uint64_t verf) override;
    void setCreateverf(std::uint64_t verf) override;
};

class NfsFsattr: public Fsattr
{
public:
    size_t tbytes() const override { return attr_.space_total_; }
    size_t fbytes() const override { return attr_.space_free_; }
    size_t abytes() const override { return attr_.space_avail_; }
    size_t tfiles() const override { return attr_.files_total_; }
    size_t ffiles() const override { return attr_.files_free_; }
    size_t afiles() const override { return attr_.files_avail_; }
    int linkMax() const override
    {
        return 0;
    }
    int nameMax() const override
    {
        return NAME_MAX;
    }

    NfsAttr attr_;
};

class NfsFile: public File, public std::enable_shared_from_this<NfsFile>
{
public:
    NfsFile(std::shared_ptr<NfsFilesystem>,
            nfs_fh4&& fh, fattr4&& attr);

    // File overrides
    std::shared_ptr<Filesystem> fs() override;
    void handle(FileHandle& fh) override;
    bool access(const Credential& cred, int accmode) override;
    std::shared_ptr<Getattr> getattr() override;
    void setattr(const Credential& cred, std::function<void(Setattr*)> cb) override;
    std::shared_ptr<File> lookup(const Credential& cred, const std::string& name) override;
    std::shared_ptr<OpenFile> open(
        const Credential& cred, const std::string& name, int flags,
        std::function<void(Setattr*)> cb) override;
    std::shared_ptr<OpenFile> open(
        const Credential& cred, int flags) override;
    std::string readlink(const Credential& cred) override;
    std::shared_ptr<File> mkdir(
        const Credential& cred, const std::string& name,
        std::function<void(Setattr*)> cb) override;
    std::shared_ptr<File> symlink(
        const Credential& cred, const std::string& name,
        const std::string& data, std::function<void(Setattr*)> cb) override;
    std::shared_ptr<File> mkfifo(
        const Credential& cred, const std::string& name,
        std::function<void(Setattr*)> cb) override;
    void remove(const Credential& cred, const std::string& name) override;
    void rmdir(const Credential& cred, const std::string& name) override;
    void rename(
        const Credential& cred, const std::string& toName,
        std::shared_ptr<File> fromDir,
        const std::string& fromName) override;
    void link(
        const Credential& cred, const std::string& name,
        std::shared_ptr<File> file) override;
    std::shared_ptr<DirectoryIterator> readdir(
        const Credential& cred, std::uint64_t seek) override;
    std::shared_ptr<Fsattr> fsstat(const Credential& cred) override;

    const nfs_fh4& fh() const { return fh_; }
    const NfsAttr& attr() const { return attr_; }
    std::shared_ptr<NfsFilesystem> nfs() const { return fs_.lock(); }
    std::shared_ptr<NfsFile> lookupp();
    std::shared_ptr<File> create(
        const createtype4& objtype, const utf8string& objname,
        std::function<void(Setattr*)> cb);

    std::shared_ptr<NfsDelegation> delegation() const
    {
        return delegation_.lock();
    }

    void clearDelegation();

    void clearCache()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cache_.clear();
    }

    std::shared_ptr<Buffer> read(
        const stateid4& stateid, std::uint64_t offset, std::uint32_t size,
        bool& eof);
    std::uint32_t write(
        const stateid4& stateid, std::uint64_t offset,
        std::shared_ptr<Buffer> data);
    void flush(const stateid4& stateid, bool commit);
    void update(fattr4&& attr);
    void recover();
    void testState();

private:
    std::mutex mutex_;
    std::weak_ptr<NfsFilesystem> fs_;
    std::weak_ptr<NfsOpenFile> open_;
    std::weak_ptr<NfsDelegation> delegation_;
    nfs_fh4 fh_;
    detail::Clock::time_point attrTime_;
    NfsAttr attr_;
    changeid4 lastChange_ = 0;
    bool haveWriteverf_ = false;
    verifier4 writeverf_;
    detail::DataCache cache_;
    bool modified_ = false;
};

class NfsOpenFile: public OpenFile
{
public:
    NfsOpenFile(
        std::shared_ptr<NfsFile> file, const stateid4& stateid, int flags)
        : file_(file),
          stateid_(stateid),
          flags_(flags)
    {
    }

    ~NfsOpenFile() override;

    // OpenFile overrides
    std::shared_ptr<File> file() const override { return file_; }
    std::shared_ptr<Buffer> read(
        std::uint64_t offset, std::uint32_t size, bool& eof) override;
    std::uint32_t write(
        std::uint64_t offset, std::shared_ptr<Buffer> data) override;
    void flush() override;

    auto stateid() const { return stateid_; }
    const nfs_fh4& fh() const { return file_->fh(); }
    std::shared_ptr<NfsFilesystem> nfs() const { return file_->nfs(); }
    auto flags() const { return flags_; }

    void setStateid(const stateid4& stateid)
    {
        stateid_ = stateid;
    }

    void setFlags(int flags)
    {
        flags_ = flags;
    }

    void setDead()
    {
        dead_ = true;
    }

private:
    std::shared_ptr<NfsFile> file_;
    stateid4 stateid_;
    int flags_;
    bool dead_ = false;
};

/// Each instance of this class tracks an active delegation
///
/// The NfsDelegation instance holds a reference to the NfsOpenFile
/// instance that created when the delegation was granted. We hold
/// this until the delegation is returned.
class NfsDelegation
{
public:
    NfsDelegation(
        std::shared_ptr<NfsFile> file, std::shared_ptr<NfsOpenFile> of,
        open_delegation4&& delegation);

    ~NfsDelegation();

    auto stateid() const { return stateid_; }
    auto isWrite() const
    {
        return delegation_.delegation_type == OPEN_DELEGATE_WRITE;
    }

private:
    std::shared_ptr<NfsFile> file_;
    std::shared_ptr<NfsOpenFile> open_;
    stateid4 stateid_;
    open_delegation4 delegation_;
};

class NfsDirectoryIterator: public DirectoryIterator
{
public:
    NfsDirectoryIterator(std::shared_ptr<NfsFile> dir, std::uint64_t seek);

    bool valid() const override;
    FileId fileid() const override;
    std::string name() const override;
    std::shared_ptr<File> file() const override;
    uint64_t seek() const override;
    void next() override;

private:
    void readdir(nfs_cookie4 cookie);

    enum {
        ISDOT,
        ISDOTDOT,
        READDIR
    } state_;

    std::shared_ptr<NfsFile> dir_;
    mutable std::shared_ptr<File> file_;
    mutable std::unique_ptr<entry4> entry_;
    mutable NfsAttr attr_;
    verifier4 verf_;
    bool eof_;
};

struct NfsFsinfo
{
    std::uint32_t maxread;
    std::uint32_t maxwrite;
};

class NfsFilesystem: public Filesystem,
                     public std::enable_shared_from_this<NfsFilesystem>
{
public:
    NfsFilesystem(
        std::shared_ptr<oncrpc::Channel> chan,
        std::shared_ptr<oncrpc::Client> client,
        std::shared_ptr<detail::Clock> clock,
        std::shared_ptr<IIdMapper> idmapper);
    NfsFilesystem(
        std::shared_ptr<oncrpc::Channel> chan,
        std::shared_ptr<oncrpc::Client> client,
        std::shared_ptr<detail::Clock> clock);
    ~NfsFilesystem();
    std::shared_ptr<File> root() override;
    const FilesystemId& fsid() const override;
    std::shared_ptr<File> find(const FileHandle& fh) override;
    void unmount() override;

    auto clientid() const { return clientid_; }
    auto sessionid() const { return sessionid_; }
    auto clock() const { return clock_; }
    auto idmapper() const { return idmapper_; }

    /// Send a compound request
    template <typename ARGS, typename RES>
    void compoundNoSequence(ARGS&& args, RES&& res)
    {
        chan_->call(
            client_.get(), NFSPROC4_COMPOUND,
            [args, this](auto xdrs) {
                CompoundRequestEncoder enc(tag_, xdrs);
                args(enc);
            },
            [res, this](auto xdrs) {
                CompoundReplyDecoder dec(tag_, xdrs);
                res(dec);
            });
    }

    /// Send a compound request with OP_SEQUENCE for Exactly Once
    /// Semantics
    template <typename ARGS, typename RES>
    void compound(ARGS&& args, RES&& res)
    {
        for (;;) {
            try {
                std::unique_lock<std::mutex> lock(mutex_);
                int slot = -1, newHighestSlot;
                while (slot == -1) {
                    int limit = std::min(
                        int(slots_.size()) - 1, highestSlot_ + 1);
                    for (int i = 0; i <= limit; i++) {
                        auto& s = slots_[i];
                        if (s.busy_) {
                            newHighestSlot = i;
                        }
                        else if (i <= targetHighestSlot_ && slot == -1) {
                            slot = i;
                            newHighestSlot = i;
                        }
                    }
                    if (slot == -1) {
                        slotWait_.wait(lock);
                        continue;
                    }
                    highestSlot_ = newHighestSlot;
                }
                auto p = std::unique_ptr<Slot, std::function<void(Slot*)>>(
                    &slots_[slot],
                    [this](Slot* p) {
                        p->busy_ = false;
                        slotWait_.notify_one();
                    });
                p->busy_ = true;
                sequenceid4 seq = p->sequence_++;
                VLOG(2) << "slot: " << slot
                        << ", highestSlot: " << highestSlot_
                        << ", sequence: " << seq;
                lock.unlock();

                int newTarget;
                bool revoked = false;
                chan_->call(
                    client_.get(), NFSPROC4_COMPOUND,
                    [&args, slot, seq, this](auto xdrs) {
                        CompoundRequestEncoder enc(tag_, xdrs);
                        enc.sequence(
                            sessionid_, seq, slot, highestSlot_, false);
                        args(enc);
                    },
                    [&res, &newTarget, &revoked, this](auto xdrs) {
                        CompoundReplyDecoder dec(tag_, xdrs);
                        auto seqres = dec.sequence();
                        newTarget = seqres.sr_target_highest_slotid;
                        constexpr int revflags =
                            SEQ4_STATUS_EXPIRED_ALL_STATE_REVOKED +
                            SEQ4_STATUS_EXPIRED_SOME_STATE_REVOKED +
                            SEQ4_STATUS_ADMIN_STATE_REVOKED +
                            SEQ4_STATUS_RECALLABLE_STATE_REVOKED;
                        if (seqres.sr_status_flags & revflags)
                            revoked = true;
                        res(dec);
                    });

                p.reset();
                if (newTarget != targetHighestSlot_) {
                    lock.lock();
                    if (slots_.size() < newTarget + 1)
                        slots_.resize(newTarget + 1);
                    targetHighestSlot_ = newTarget;
                    lock.unlock();
                }
                if (revoked)
                    freeRevokedState();
                return;
            }
            catch (nfsstat4 st) {
                using namespace std::literals;
                switch (st) {
                case NFS4ERR_DELAY:
                    std::this_thread::sleep_for(1ms);
                    continue;
                case NFS4ERR_GRACE:
                    std::this_thread::sleep_for(5s);
                    continue;
                case NFS4ERR_BADSESSION:
                case NFS4ERR_DEADSESSION:
                    connect();
                    continue;
                default:
                    throw mapStatus(st);
                }
            }
        }
    }

    // Find or create an NfsFile instance that corresponds to the
    // given filehandle
    std::shared_ptr<NfsFile> find(const nfs_fh4& fh);
    std::shared_ptr<NfsFile> find(nfs_fh4&& fh, fattr4&& attr);

    // Add a new delegation
    std::shared_ptr<NfsDelegation> addDelegation(
        std::shared_ptr<NfsFile> file, std::shared_ptr<NfsOpenFile> of,
        open_delegation4&& delegation);

    // Drop our reference to a delegation, triggering its return
    void clearDelegation(const stateid4& stateid)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        delegations_.remove(stateid);
    }

    const NfsFsinfo& fsinfo() const { return fsinfo_; }

    // Strictly for unit-testing - decrement the sequence number in a
    // slot to test the server replay cache
    void forceReplay(int slot) {
        slots_[slot].sequence_--;
    }

private:
    // Connect to server, establishing clientid and session
    void connect();

    // Iterate over current state and identify any revoked state
    void freeRevokedState();

    // Service incoming RPC messages on the back channel
    void handleCallbacks();

    struct Slot {
        sequenceid4 sequence_ = 1;
        bool busy_ = false;
    };

    std::mutex mutex_;
    std::shared_ptr<oncrpc::Channel> chan_;
    std::shared_ptr<oncrpc::Client> client_;
    std::shared_ptr<detail::Clock> clock_;
    std::shared_ptr<IIdMapper> idmapper_;
    std::string tag_;
    client_owner4 clientOwner_;
    clientid4 clientid_ = 0;
    sequenceid4 sequence_;             // sequence of the client pseudo slot
    sessionid4 sessionid_;
    std::vector<Slot> slots_;
    int highestSlot_;                  // current highest slot in-use
    int targetHighestSlot_;            // server's target slot limit
    std::condition_variable slotWait_; // wait for free slot
    std::shared_ptr<NfsFile> root_;
    NfsFsinfo fsinfo_;
    detail::LRUCache<nfs_fh4, NfsFile, NfsFhHash> cache_;
    detail::LRUCache<
        stateid4, NfsDelegation,
        NfsStateidHashIgnoreSeqid,
        NfsStateidEqualIgnoreSeqid> delegations_;

    // Callback service support
    std::shared_ptr<oncrpc::SocketManager> sockman_;
    std::shared_ptr<oncrpc::ServiceRegistry> svcreg_;
    uint32_t cbprog_;
    std::thread cbthread_;
    NfsCallbackService cbsvc_;
};

class NfsFilesystemFactory: public FilesystemFactory
{
public:
    std::string name() const override { return "nfs"; }
    std::pair<std::shared_ptr<Filesystem>, std::string> mount(
        FilesystemManager* fsman, const std::string& url) override;
};

void init(FilesystemManager* fsman);

}
}
