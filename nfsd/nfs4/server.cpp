#include <pwd.h>
#include <grp.h>
#include <iomanip>
#include <sstream>

#include <fs++/filesys.h>
#include <rpc++/cred.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "server.h"
#include "session.h"
#include "util.h"

using namespace filesys;
using namespace filesys::nfs4;
using namespace nfsd;
using namespace nfsd::nfs4;
using namespace oncrpc;
using namespace std;
using namespace chrono;

DECLARE_int32(iosize);
DECLARE_int32(grace_time);
DECLARE_int32(lease_time);

static nfsstat4 exportStatus(const system_error& e)
{
    static unordered_map<int, int> statusMap = {
        { EPERM, NFS4ERR_PERM },
        { ENOENT, NFS4ERR_NOENT },
        { EIO, NFS4ERR_IO },
        { ENXIO, NFS4ERR_NXIO },
        { EACCES, NFS4ERR_ACCESS },
        { EEXIST, NFS4ERR_EXIST },
        { EXDEV, NFS4ERR_XDEV },
        { ENODEV, NFS4ERR_XDEV },
        { ENOTDIR, NFS4ERR_NOTDIR },
        { EISDIR, NFS4ERR_ISDIR },
        { EINVAL, NFS4ERR_INVAL },
        { EFBIG, NFS4ERR_FBIG },
        { ENOSPC, NFS4ERR_NOSPC },
        { EROFS, NFS4ERR_ROFS },
        { EMLINK, NFS4ERR_MLINK },
        { ENAMETOOLONG, NFS4ERR_NAMETOOLONG },
        { ENOTEMPTY, NFS4ERR_NOTEMPTY },
        { EDQUOT, NFS4ERR_DQUOT },
        { ESTALE, NFS4ERR_STALE },
        { EOPNOTSUPP, NFS4ERR_NOTSUPP },
    };
    int error = e.code().value();
    auto i = statusMap.find(error);
    if (i != statusMap.end())
           return nfsstat4(i->second);
    else
           return NFS4ERR_INVAL;
}

NfsServer::NfsServer(
    const vector<int>& sec,
    shared_ptr<IIdMapper> idmapper,
    shared_ptr<detail::Clock> clock)
    : sec_(sec),
      idmapper_(idmapper),
      clock_(clock)
{
    // Set the grace period expiry to the default lease_time value
    graceExpiry_ = clock_->now() + seconds(FLAGS_grace_time);

    char hostname[256];
    if (::gethostname(hostname, sizeof(hostname)) < 0)
        throw system_error(errno, system_category());
    ostringstream ss;
    ss << "unfsd" << ::getpid() << "@" << hostname;
    auto s = ss.str();
    owner_.so_major_id.resize(s.size());
    copy_n(s.data(), s.size(), owner_.so_major_id.data());
    owner_.so_minor_id = 1;

    // Use the server state time as write verifier
    auto d = duration_cast<nanoseconds>(system_clock::now().time_since_epoch());
    *(uint64_t*) writeverf_.data() = d.count();

    // Also use time in seconds as the base for generating new client IDs
    nextClientId_ = uint64_t(d.count() / 1000000000) << 32;
}

NfsServer::NfsServer(const vector<int>& sec)
    : NfsServer(sec, LocalIdMapper(), make_shared<detail::SystemClock>())
{
}

void NfsServer::null()
{
}

ACCESS4res NfsServer::access(
    CompoundState& state,
    const ACCESS4args& args)
{
    auto& cred = CallContext::current().cred();
    VLOG(1) << "NfsServer::access(" << args.access << ")";

    if (!state.curr.file)
        return ACCESS4res(NFS4ERR_NOFILEHANDLE);
    try {
        static unordered_map<uint32_t, int> accmap = {
            {ACCESS4_READ, int(AccessFlags::READ)},
            {ACCESS4_LOOKUP, int(AccessFlags::EXECUTE)},
            {ACCESS4_MODIFY, int(AccessFlags::WRITE)},
            {ACCESS4_EXTEND, int(AccessFlags::WRITE)},
            {ACCESS4_EXECUTE, int(AccessFlags::EXECUTE)},
        };

        // We don't support ACCESS4_DELETE - delete permission is granted
        // by the directory containing the file.
        uint32_t requested = args.access;
        requested &= ~ACCESS4_DELETE;

        // First try checking all the flags - if this succeeds, we are done
        uint32_t access = requested, supported = 0;
        int accmode = 0;
        while (access) {
            uint32_t abit = access & ~(access - 1);
            access &= ~abit;
            auto i = accmap.find(abit);
            if (i != accmap.end()) {
                accmode |= i->second;
                supported |= abit;
            }
        }
        if (state.curr.file->access(cred, accmode)) {
            return ACCESS4res{NFS4_OK, ACCESS4resok{supported, args.access}};
        }

        // Otherwise, we need to check each bit separately
        ACCESS4res res{NFS4_OK, ACCESS4resok{supported, 0}};
        access = requested;
        while (access) {
            uint32_t abit = access & ~(access - 1);
            access &= ~abit;
            auto i = accmap.find(abit);
            if (i != accmap.end() && state.curr.file->access(cred, i->second))
                res.resok4().access |= abit;
        }
        return res;
    }
    catch (system_error& e) {
        return ACCESS4res(exportStatus(e));
    }
}

CLOSE4res NfsServer::close(
    CompoundState& state,
    const CLOSE4args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::close("
                << args.seqid
                << ", " << args.open_stateid << ")";

    if (!state.curr.file)
        return CLOSE4res(NFS4ERR_NOFILEHANDLE);
    auto client = state.session->client();

    try {
        auto ns = client->findState(state, args.open_stateid);
        if (ns->type() != NfsState::OPEN)
            return CLOSE4res(NFS4ERR_BAD_STATEID);
        client->clearState(args.open_stateid);
        unique_lock<mutex> lock(mutex_);
        auto fs = findState(lock, state.curr.file);
        fs->removeOpen(ns);
        if (!fs->hasState())
            files_.erase(state.curr.file);
        auto id = STATEID_INVALID;
        return CLOSE4res(NFS4_OK, move(id));
    }
    catch (nfsstat4 status) {
        return CLOSE4res(status);
    }
}

COMMIT4res NfsServer::commit(
    CompoundState& state,
    const COMMIT4args& args)
{
    auto& cred = CallContext::current().cred();
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::commit("
                << args.offset
                << ", " << args.count << ")";
    try {
        // XXX: offset, count
        state.curr.file->open(cred, OpenFlags::WRITE)->flush();
        return COMMIT4res(NFS4_OK, COMMIT4resok{writeverf_});
    }
    catch (system_error& e) {
        return COMMIT4res(exportStatus(e));
    }
}

CREATE4res NfsServer::create(
    CompoundState& state,
    const CREATE4args& args)
{
    auto& cred = CallContext::current().cred();
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::create("
                << args.objtype.type
                << ", " << toString(args.objname) << ", ...)";

    if (!state.curr.file)
        return CREATE4res(NFS4ERR_NOFILEHANDLE);
    try {
        auto dir = state.curr.file;
        CREATE4resok resok;
        resok.cinfo.atomic = false;
        resok.cinfo.before = dir->getattr()->change();
        switch (args.objtype.type) {
        case NF4DIR:
            state.curr.file = state.curr.file->mkdir(
                cred, toString(args.objname),
                importAttr(args.createattrs, resok.attrset));
            break;
        case NF4LNK:
            state.curr.file = state.curr.file->symlink(
                cred, toString(args.objname),
                toString(args.objtype.linkdata()),
                importAttr(args.createattrs, resok.attrset));
            break;
        case NF4FIFO:
            state.curr.file = state.curr.file->mkfifo(
                cred, toString(args.objname),
                importAttr(args.createattrs, resok.attrset));
            break;
        default:
            return CREATE4res(NFS4ERR_BADTYPE);
        }
        state.curr.stateid = STATEID_ANON;
        resok.cinfo.after = dir->getattr()->change();
        return CREATE4res(NFS4_OK, move(resok));
    }
    catch (system_error& e) {
        return CREATE4res(exportStatus(e));
    }
}

DELEGPURGE4res NfsServer::delegpurge(
    CompoundState& state,
    const DELEGPURGE4args& args)
{
    return DELEGPURGE4res{NFS4ERR_NOTSUPP};
}

DELEGRETURN4res NfsServer::delegreturn(
    CompoundState& state,
    const DELEGRETURN4args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::delegreturn(" << args.deleg_stateid << ")";

    if (!state.curr.file)
        return DELEGRETURN4res{NFS4ERR_NOFILEHANDLE};
    auto client = state.session->client();
    try {
        auto ns = client->findState(state, args.deleg_stateid);
        if (ns->type() != NfsState::DELEGATION)
            return DELEGRETURN4res{NFS4ERR_BAD_STATEID};
        client->clearState(args.deleg_stateid);
        checkState(state.curr.file);
        return DELEGRETURN4res{NFS4_OK};
    }
    catch (nfsstat4 status) {
        return DELEGRETURN4res{status};
    }
}

GETATTR4res NfsServer::getattr(
    CompoundState& state,
    const GETATTR4args& args)
{
    VLOG(1) << "NfsServer::getattr()";
    if (state.curr.file) {
        return GETATTR4res(
            NFS4_OK,
            GETATTR4resok{exportAttr(state.curr.file, args.attr_request)});
    }
    else {
        return GETATTR4res(NFS4ERR_NOFILEHANDLE);
    }
}

GETFH4res NfsServer::getfh(
    CompoundState& state)
{
    VLOG(1) << "NfsServer::getfh()";
    if (state.curr.file)
        return GETFH4res(
            NFS4_OK, GETFH4resok{exportFileHandle(state.curr.file)});
    else
        return GETFH4res(NFS4ERR_NOFILEHANDLE);
}

LINK4res NfsServer::link(
    CompoundState& state,
    const LINK4args& args)
{
    auto& cred = CallContext::current().cred();
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::link("
                << toString(args.newname) << ")";

    if (!state.curr.file || !state.save.file)
        return LINK4res(NFS4ERR_NOFILEHANDLE);
    try {
        LINK4resok resok;
        resok.cinfo.atomic = false;
        resok.cinfo.before = state.curr.file->getattr()->change();
        state.curr.file->link(cred, toString(args.newname), state.save.file);
        resok.cinfo.after = state.curr.file->getattr()->change();
        return LINK4res(NFS4_OK, move(resok));
    }
    catch (system_error& e) {
        return LINK4res(exportStatus(e));
    }
}

LOCK4res NfsServer::lock(
    CompoundState& state,
    const LOCK4args& args)
{
    return LOCK4res(NFS4ERR_NOTSUPP);
}

LOCKT4res NfsServer::lockt(
    CompoundState& state,
    const LOCKT4args& args)
{
    return LOCKT4res(NFS4ERR_NOTSUPP);
}

LOCKU4res NfsServer::locku(
    CompoundState& state,
    const LOCKU4args& args)
{
    return LOCKU4res(NFS4ERR_NOTSUPP);
}

LOOKUP4res NfsServer::lookup(
    CompoundState& state,
    const LOOKUP4args& args)
{
    auto& cred = CallContext::current().cred();
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::lookup("
                << toString(args.objname) << ")";
    if (!state.curr.file)
        return LOOKUP4res{NFS4ERR_NOFILEHANDLE};
    try {
        state.curr.file = state.curr.file->lookup(cred, toString(args.objname));
        state.curr.stateid = STATEID_ANON;
        return LOOKUP4res{NFS4_OK};
    }
    catch (system_error& e) {
        return LOOKUP4res{exportStatus(e)};
    }
}

LOOKUPP4res NfsServer::lookupp(
    CompoundState& state)
{
    auto& cred = CallContext::current().cred();
    VLOG(1) << "NfsServer::lookupp()";
    if (!state.curr.file)
        return LOOKUPP4res{NFS4ERR_NOFILEHANDLE};
    try {
        state.curr.file = state.curr.file->lookup(cred, "..");
        state.curr.stateid = STATEID_ANON;
        return LOOKUPP4res{NFS4_OK};
    }
    catch (system_error& e) {
        return LOOKUPP4res{exportStatus(e)};
    }
}

NVERIFY4res NfsServer::nverify(
    CompoundState& state,
    const NVERIFY4args& args)
{
    VLOG(1) << "NfsServer::nverify(...)";
    if (!state.curr.file)
        return NVERIFY4res{NFS4ERR_NOFILEHANDLE};
    auto status = verifyAttr(state.curr.file, args.obj_attributes);
    if (status == NFS4ERR_NOT_SAME)
        return NVERIFY4res{NFS4_OK};
    else
        return NVERIFY4res{status};
}

OPEN4res NfsServer::open(
    CompoundState& state,
    const OPEN4args& args)
{
    auto& cred = CallContext::current().cred();
    if (VLOG_IS_ON(1)) {
        string name = "<curfh>";
        if (args.claim.claim == CLAIM_NULL)
            name = toString(args.claim.file());
        VLOG(1) << "NfsServer::open("
                << name
                << ", " << args.share_access
                << ", " << args.share_deny
                << ", " << args.owner
                << ", ...)";
    }
    if (!state.curr.file)
        return OPEN4res(NFS4ERR_NOFILEHANDLE);
    try {
        auto client = state.session->client();

        // Client must specify either read or write or both
        if (!(args.share_access & OPEN4_SHARE_ACCESS_BOTH))
            return OPEN4res(NFS4ERR_INVAL);

        auto share_access = args.share_access & OPEN4_SHARE_ACCESS_BOTH;
        auto share_deny = args.share_deny & OPEN4_SHARE_DENY_BOTH;
        auto want_deleg =
            args.share_access & OPEN4_SHARE_ACCESS_WANT_DELEG_MASK;

        int flags = 0;
        const fattr4* attrp = nullptr;
        const verifier4* verfp = nullptr;

        if (share_access & OPEN4_SHARE_ACCESS_READ)
            flags |= OpenFlags::READ;
        if (share_access & OPEN4_SHARE_ACCESS_WRITE)
            flags |= OpenFlags::WRITE;
        if (args.openhow.opentype == OPEN4_CREATE) {
            switch (args.openhow.how().mode) {
            case GUARDED4:
                flags |= OpenFlags::EXCLUSIVE;
            case UNCHECKED4:
                attrp = &args.openhow.how().createattrs();
                break;

            case EXCLUSIVE4:
                verfp = &args.openhow.how().createverf();
                break;

            case EXCLUSIVE4_1:
                verfp = &args.openhow.how().ch_createboth().cva_verf;
                attrp = &args.openhow.how().ch_createboth().cva_attrs;
                break;
            }
            if (!verfp)
                flags |= OpenFlags::CREATE;
        }

        uint64_t verf = 0;
        if (verfp) {
            verf = *reinterpret_cast<const uint64_t*>(verfp->data());
        }

        // Serialize open and layoutget so that we can ensure that we
        // handle any state conflicts correctly
        unique_lock<mutex> lk(mutex_);

        // See if the requested file exists so that we can check for conflicts
        shared_ptr<File> dir = state.curr.file;
        shared_ptr<File> file;
        switch (args.claim.claim) {
        case CLAIM_NULL: {
            auto name = toString(args.claim.file());
            try {
                file = dir->lookup(cred, name);
            }
            catch (system_error&) {
            }
            break;
        }

        case CLAIM_FH:
        case CLAIM_PREVIOUS:
            file = state.curr.file;
            break;

        default:
            LOG(ERROR) << "Unsupported open claim: " << args.claim.claim;
            return OPEN4res(NFS4ERR_NOTSUPP);
        }

        shared_ptr<NfsFileState> fs;
        if (file) {
            fs = findState(lk, file);

            // Check existing share reservations
            VLOG(1) << "Checking existing share reservations";
            if (!fs->checkShare(
                    client, args.owner, share_access, share_deny))
                return OPEN4res(NFS4ERR_SHARE_DENIED);

            // Recall any conflicting delegations or layouts
            vector<shared_ptr<NfsState>> toRecall;
            if (share_access & OPEN4_SHARE_ACCESS_WRITE) {
                VLOG(1) << "Checking for conflicting read or write state";
                for (auto ns: fs->delegations()) {
                    VLOG(1) << "Checking delegation " << ns->id();
                    if (ns->owner() != args.owner) {
                        toRecall.push_back(ns);
                    }
                }
                for (auto ns: fs->layouts()) {
                    VLOG(1) << "Checking layout " << ns->id();
                    if (ns->owner().clientid != client->id()) {
                        toRecall.push_back(ns);
                    }
                }
            }
            if (share_access & OPEN4_SHARE_ACCESS_READ) {
                VLOG(1) << "Checking for conflicting write state";
                for (auto ns: fs->delegations()) {
                    VLOG(1) << "Checking delegation " << ns->id();
                    if (ns->access() & OPEN4_SHARE_ACCESS_WRITE) {
                        if (ns->owner() != args.owner) {
                            toRecall.push_back(ns);
                        }
                    }
                }
                for (auto ns: fs->layouts()) {
                    VLOG(1) << "Checking layout " << ns->id();
                    if (ns->access() & OPEN4_SHARE_ACCESS_WRITE) {
                        if (ns->owner().clientid != client->id()) {
                            toRecall.push_back(ns);
                        }
                    }
                }
            }

            // If we issued any recalls, ask our caller to re-try after a
            // short delay
            if (toRecall.size() > 0) {
                for (auto ns: toRecall)
                    ns->recall();
                VLOG(1) << "Delaying open due to recalls";
                return OPEN4res(NFS4ERR_DELAY);
            }
        }

        // There are no state conflicts so go head and open or create
        // the file and apply any attribute changes
        OPEN4resok resok;
        resok.cinfo.atomic = false;
        resok.cinfo.before = dir->getattr()->change();
        shared_ptr<OpenFile> of;
        bitmap4 attrsset;

        switch (args.claim.claim) {
        case CLAIM_NULL: {
            if (inGracePeriod())
                return OPEN4res(NFS4ERR_GRACE);

            auto name = toString(args.claim.file());
            auto attrcb =
                attrp ? importAttr(*attrp, attrsset) : [](Setattr*){};

            if (verfp) {
                try {
                    of = dir->open(cred, name, flags, [](auto){});
                    if (of->file()->getattr()->createverf() != verf)
                        throw system_error(EEXIST, system_category());
                }
                catch (system_error& e) {
                    if (e.code().value() == ENOENT) {
                        of = dir->open(
                            cred, name, flags | OpenFlags::CREATE,
                            [verf, &attrcb](auto sattr) {
                                sattr->setCreateverf(verf);
                                attrcb(sattr);
                            });
                    }
                    else {
                        throw;
                    }
                }
            }
            else {
                of = dir->open(cred, name, flags, attrcb);
            }
            state.curr.file = of->file();
            break;
        }
        case CLAIM_FH:
            if (inGracePeriod())
                return OPEN4res(NFS4ERR_GRACE);
            of = dir->open(cred, flags);
            break;

        case CLAIM_PREVIOUS:
            if (!inGracePeriod())
                return OPEN4res(NFS4ERR_NO_GRACE);
            if (args.claim.delegate_type() != OPEN_DELEGATE_NONE) {
                LOG(ERROR) << "Unsupported delegate_type for CLAIM_PREVIOUS: "
                           << args.claim.delegate_type();
                return OPEN4res(NFS4ERR_RECLAIM_BAD);
            }
            of = dir->open(cred, flags);
            break;

        default:
            LOG(ERROR) << "Unsupported open claim: " << args.claim.claim;
            return OPEN4res(NFS4ERR_NOTSUPP);
        }

        // Find the state tracking object for the opened file if we
        // don't already have it
        if (file) {
            assert(state.curr.file == file);
        }
        else {
            fs = findState(lk, state.curr.file);
        }

        // Check for open upgrade
        auto ns = fs->findOpen(client, args.owner);
        if (ns) {
            ns->updateOpen(share_access, share_deny, of);
            fs->updateShare(ns);
        }

        // See if a delegation possible (and wanted)
        bool issueDelegation = false;
        open_delegation4 delegation = open_delegation4(OPEN_DELEGATE_NONE);
        auto deleg = fs->findDelegation(client);
        switch (want_deleg) {
        case OPEN4_SHARE_ACCESS_WANT_NO_PREFERENCE:
            break;

        case OPEN4_SHARE_ACCESS_WANT_NO_DELEG:
            delegation = open_delegation4(
                OPEN_DELEGATE_NONE_EXT,
                open_none_delegation4(WND4_NOT_WANTED));
            break;

        case OPEN4_SHARE_ACCESS_WANT_CANCEL:
            delegation = open_delegation4(
                OPEN_DELEGATE_NONE_EXT,
                open_none_delegation4(WND4_CANCELLED));
            break;

        case OPEN4_SHARE_ACCESS_WANT_READ_DELEG:
        tryRead:
            // If we already have a delegation, this may be a request
            // for an atomic downgrade
            if (deleg) {
                issueDelegation = true;
                deleg->updateDelegation(OPEN4_SHARE_ACCESS_READ, of);
                VLOG(1) << "Downgrading to read delegation: " << deleg->id();
            }
            else {
                // We can issue a read delegation if there are no
                // existing write delegations or write opens.
                issueDelegation = true;
                for (auto ns: fs->delegations()) {
                    if (ns->access() & OPEN4_SHARE_ACCESS_WRITE) {
                        VLOG(1) << "Denying read delegation: "
                                << "existing write delegation";
                        delegation = open_delegation4(
                            OPEN_DELEGATE_NONE_EXT,
                            open_none_delegation4(WND4_CONTENTION, false));
                        issueDelegation = false;
                        break;
                    }
                }
                for (auto ns: fs->opens()) {
                    if (ns->access() & OPEN4_SHARE_ACCESS_WRITE) {
                        VLOG(1) << "Denying read delegation: "
                                << "existing write open";
                        delegation = open_delegation4(
                            OPEN_DELEGATE_NONE_EXT,
                            open_none_delegation4(WND4_CONTENTION, false));
                        issueDelegation = false;
                        break;
                    }
                }
            }
            break;

        case OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG:
            // If we already have a delegation, this may be a request
            // for an atomic upgrade
            if (deleg) {
                issueDelegation = true;
                deleg->updateDelegation(OPEN4_SHARE_ACCESS_WRITE, of);
                VLOG(1) << "Upgrading to write delegation: " << deleg->id();
            }
            else {
                /// We can issue a write delegation if there are no
                /// existing delegations or opens at all
                if (fs->delegations().size() == 0 && fs->opens().size() == 0)
                    issueDelegation = true;
                VLOG(1) << "Denying write delegation: "
                        << "existing open or delegation";
                delegation = open_delegation4(
                    OPEN_DELEGATE_NONE_EXT,
                    open_none_delegation4(WND4_CONTENTION, false));
            }
            break;

        case OPEN4_SHARE_ACCESS_WANT_ANY_DELEG:
            // If we already have a delegation, just keep it as-is
            if (deleg) {
                issueDelegation = true;
                break;
            }
            // Issue a write delegation if there are no existing
            // delegations or opens, otherwise try to issue a read
            // delegation
            if (fs->delegations().size() == 0 && fs->opens().size() == 0) {
                want_deleg = OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG;
                issueDelegation = true;
            }
            else {
                want_deleg = OPEN4_SHARE_ACCESS_WANT_READ_DELEG;
                goto tryRead;
            }
            break;

        default:
            delegation = open_delegation4(
                OPEN_DELEGATE_NONE_EXT,
                open_none_delegation4(WND4_NOT_WANTED));
        }

        // We only support delegating access to files
        if (issueDelegation &&
            state.curr.file->getattr()->type() != FileType::FILE) {
            assert(!deleg);
            issueDelegation = false;
            if (state.curr.file->getattr()->type() == FileType::DIRECTORY)
                delegation = open_delegation4(
                    OPEN_DELEGATE_NONE_EXT,
                    open_none_delegation4(WND4_IS_DIR));
            else
                delegation = open_delegation4(
                    OPEN_DELEGATE_NONE_EXT,
                    open_none_delegation4(WND4_NOT_SUPP_FTYPE));
        }

        // We can't issue any delegation if the back channel doesn't work
        if (issueDelegation && !state.session->testBackchannel()) {
            issueDelegation = false;
        }

        resok.cinfo.after = dir->getattr()->change();
        if (!ns) {
            ns = client->addOpen(
                fs, args.owner, share_access, share_deny, of);
        }
        state.curr.stateid = ns->id();
        fs->addOpen(ns);

        VLOG(1) << "Returning open stateid: " << ns->id();
        resok.stateid = ns->id();
        // XXX: fix objfs to allow setting OPEN4_RESULT_PRESERVE_UNLINKED
        resok.rflags = OPEN4_RESULT_LOCKTYPE_POSIX;
        resok.attrset = move(attrsset);
        if (issueDelegation) {
            if (!deleg) {
                assert(want_deleg == OPEN4_SHARE_ACCESS_WANT_READ_DELEG ||
                       want_deleg == OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG);
                if (want_deleg == OPEN4_SHARE_ACCESS_WANT_READ_DELEG) {
                    deleg = client->addDelegation(
                        fs, args.owner, OPEN4_SHARE_ACCESS_READ, of);
                    VLOG(1) << "Creating read delegation: " << deleg->id();
                    delegation = open_delegation4(
                        OPEN_DELEGATE_READ,
                        open_read_delegation4{
                            deleg->id(),
                                false,
                                nfsace4{ACE4_ACCESS_ALLOWED_ACE_TYPE, 0,
                                    ACE4_GENERIC_READ,
                                    toUtf8string("OWNER@")}});
                }
                else {
                    deleg = client->addDelegation(
                        fs, args.owner, OPEN4_SHARE_ACCESS_WRITE, of);
                    VLOG(1) << "Creating write delegation: " << deleg->id();
                    delegation = open_delegation4(
                        OPEN_DELEGATE_WRITE,
                        open_write_delegation4{
                            deleg->id(),
                                false,
                                nfs_space_limit4(NFS_LIMIT_SIZE, ~0ul),
                                nfsace4{ACE4_ACCESS_ALLOWED_ACE_TYPE, 0,
                                    ACE4_GENERIC_WRITE,
                                    toUtf8string("OWNER@")}});
                }
                fs->addDelegation(deleg);
            }
        }
        else {
            deleg.reset();
        }
        if (deleg) {
            if (deleg->access() == OPEN4_SHARE_ACCESS_READ) {
                delegation = open_delegation4(
                    OPEN_DELEGATE_READ,
                    open_read_delegation4{
                        deleg->id(),
                        false,
                        nfsace4{ACE4_ACCESS_ALLOWED_ACE_TYPE, 0,
                                ACE4_GENERIC_READ, toUtf8string("OWNER@")}});
            }
            else {
                delegation = open_delegation4(
                    OPEN_DELEGATE_WRITE,
                    open_write_delegation4{
                        deleg->id(),
                        false,
                        nfs_space_limit4(NFS_LIMIT_SIZE, ~0ul),
                        nfsace4{ACE4_ACCESS_ALLOWED_ACE_TYPE, 0,
                                ACE4_GENERIC_WRITE, toUtf8string("OWNER@")}});
            }
        }

        resok.delegation = move(delegation);
        return OPEN4res(NFS4_OK, move(resok));
    }
    catch (system_error& e) {
        return OPEN4res(exportStatus(e));
    }
}

OPENATTR4res NfsServer::openattr(
    CompoundState& state,
    const OPENATTR4args& args)
{
    return OPENATTR4res{NFS4ERR_NOTSUPP};
}

OPEN_CONFIRM4res NfsServer::open_confirm(
    CompoundState& state,
    const OPEN_CONFIRM4args& args)
{
    return OPEN_CONFIRM4res(NFS4ERR_NOTSUPP);
}

OPEN_DOWNGRADE4res NfsServer::open_downgrade(
    CompoundState& state,
    const OPEN_DOWNGRADE4args& args)
{
    return OPEN_DOWNGRADE4res(NFS4ERR_NOTSUPP);
}

PUTFH4res NfsServer::putfh(
    CompoundState& state,
    const PUTFH4args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::putfh(" << args.object << ")";
    try {
        state.curr.file = importFileHandle(args.object);
        state.curr.stateid = STATEID_ANON;
        return PUTFH4res{NFS4_OK};
    }
    catch (system_error& e) {
        return PUTFH4res{exportStatus(e)};
    }
}

PUTPUBFH4res NfsServer::putpubfh(
    CompoundState& state)
{
    VLOG(1) << "NfsServer::putpubfh()";
    state.curr.file = FilesystemManager::instance().begin()->second->root();
    state.curr.stateid = STATEID_ANON;
    return PUTPUBFH4res{NFS4_OK};
}

PUTROOTFH4res NfsServer::putrootfh(
    CompoundState& state)
{
    VLOG(1) << "NfsServer::putrootfh()";
    state.curr.file = FilesystemManager::instance().begin()->second->root();
    state.curr.stateid = STATEID_ANON;
    return PUTROOTFH4res{NFS4_OK};
}

READ4res NfsServer::read(
    CompoundState& state,
    const READ4args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::read("
                << args.stateid
                << ", " << args.offset
                << ", " << args.count << ")";

    if (!state.curr.file)
        return READ4res(NFS4ERR_NOFILEHANDLE);

    if (inGracePeriod())
        return READ4res(NFS4ERR_GRACE);

    auto client = state.session->client();
    try {
        shared_ptr<OpenFile> of;
        if (args.stateid == STATEID_ANON) {
            auto& cred = CallContext::current().cred();
            auto fs = findState(state.curr.file);
            if (fs->deny() & OPEN4_SHARE_ACCESS_READ)
                return READ4res(NFS4ERR_PERM);
            of = state.curr.file->open(cred, OpenFlags::READ);
        }
        else {
            auto ns = client->findState(state, args.stateid);
            if ((ns->access() & OPEN4_SHARE_ACCESS_READ) == 0)
                return READ4res(NFS4ERR_OPENMODE);
            of = ns->of();
        }
        bool eof;
        auto data = of->read(args.offset, args.count, eof);
        return READ4res(NFS4_OK, READ4resok{eof, data});
    }
    catch (nfsstat4 status) {
        return READ4res(status);
    }
    catch (system_error& e) {
        return READ4res(exportStatus(e));
    }
}

READDIR4res NfsServer::readdir(
    CompoundState& state,
    const READDIR4args& args)
{
    auto& cred = CallContext::current().cred();
    VLOG(1) << "NfsServer::readdir(...)";

    if (!state.curr.file)
        return READDIR4res(NFS4ERR_NOFILEHANDLE);

    try {
        // XXX: cookieverf
        READDIR4res res;
        res.set_status(NFS4_OK);
        res.resok4().cookieverf = args.cookieverf;
        res.resok4().reply.eof = true;
        count4 replySize = XdrSizeof(res);
        count4 dirSize = 0;
        unique_ptr<entry4>* entryp = &res.resok4().reply.entries;
        for (auto iter = state.curr.file->readdir(cred, args.cookie);
            iter->valid(); iter->next()) {
            // Skip "." and ".." entries
            if (iter->name() == "." || iter->name() == "..")
                continue;
            auto entry = make_unique<entry4>();
            entry->cookie = iter->seek();
            entry->name = toUtf8string(iter->name());
            auto file = iter->file();
            entry->attrs = exportAttr(file, args.attr_request);

            // Calculate the full entry size as well as the size of just
            // the directory information
            auto dirEntrySize =
                XdrSizeof(entry->cookie) + XdrSizeof(entry->name);
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
                res.resok4().reply.eof = false;
                break;
            }
        }
        return res;
    }
    catch (system_error& e) {
        return READDIR4res(exportStatus(e));
    }
}

READLINK4res NfsServer::readlink(
    CompoundState& state)
{
    auto& cred = CallContext::current().cred();
    VLOG(1) << "NfsServer::readlink()";

    if (!state.curr.file)
        return READLINK4res(NFS4ERR_NOFILEHANDLE);
    try {
        return READLINK4res(
            NFS4_OK,
            READLINK4resok{toUtf8string(state.curr.file->readlink(cred))});
    }
    catch (system_error& e) {
        return READLINK4res(exportStatus(e));
    }
}

REMOVE4res NfsServer::remove(
    CompoundState& state,
    const REMOVE4args& args)
{
    auto& cred = CallContext::current().cred();
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::remove(" << toString(args.target) << ")";
    if (!state.curr.file)
        return REMOVE4res(NFS4ERR_NOFILEHANDLE);
    try {
        auto name = toString(args.target);
        auto obj = state.curr.file->lookup(cred, name);
        auto before = state.curr.file->getattr()->change();
        if (obj->getattr()->type() == FileType::DIRECTORY)
            state.curr.file->rmdir(cred, name);
        else
            state.curr.file->remove(cred, name);
        auto after = state.curr.file->getattr()->change();
        return REMOVE4res(NFS4_OK, REMOVE4resok{{false, before, after}});
    }
    catch (system_error& e) {
        return REMOVE4res(exportStatus(e));
    }
}

RENAME4res NfsServer::rename(
    CompoundState& state,
    const RENAME4args& args)
{
    auto& cred = CallContext::current().cred();
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::rename("
                << toString(args.oldname)
                << ", " << toString(args.newname) << ")";

    if (!state.curr.file || !state.save.file)
        return RENAME4res(NFS4ERR_NOFILEHANDLE);
    try {
        auto oldname = toString(args.oldname);
        auto newname = toString(args.newname);
        RENAME4resok resok;
        resok.source_cinfo.atomic = false;
        resok.source_cinfo.before = state.save.file->getattr()->change();
        resok.target_cinfo.atomic = false;
        resok.target_cinfo.before = state.curr.file->getattr()->change();
        state.curr.file->rename(cred, newname, state.save.file, oldname);
        resok.source_cinfo.after = state.save.file->getattr()->change();
        resok.target_cinfo.after = state.curr.file->getattr()->change();
        return RENAME4res(NFS4_OK, move(resok));
    }
    catch (system_error& e) {
        return RENAME4res(exportStatus(e));
    }
}

RENEW4res NfsServer::renew(
    CompoundState& state,
    const RENEW4args& args)
{
    return RENEW4res{NFS4ERR_NOTSUPP};
}

RESTOREFH4res NfsServer::restorefh(
    CompoundState& state)
{
    VLOG(1) << "NfsServer::restorefh()";
    if (state.save.file) {
        state.curr = state.save;
        return RESTOREFH4res{NFS4_OK};
    }
    else {
        return RESTOREFH4res{NFS4ERR_NOFILEHANDLE};
    }
}

SAVEFH4res NfsServer::savefh(
    CompoundState& state)
{
    VLOG(1) << "NfsServer::savefh()";
    if (state.curr.file) {
        state.save = state.curr;
        return SAVEFH4res{NFS4_OK};
    }
    else {
        return SAVEFH4res{NFS4ERR_NOFILEHANDLE};
    }
}

SECINFO4res NfsServer::secinfo(
    CompoundState& state,
    const SECINFO4args& args)
{
    VLOG(1) << "NfsServer::secinfo(" << args.name << ")";
    if (!state.curr.file)
        return SECINFO4res(NFS4ERR_NOFILEHANDLE);
    try {
        auto& cred = CallContext::current().cred();
        state.curr.file->lookup(cred, toString(args.name));
    }
    catch (system_error& e) {
        return SECINFO4res(exportStatus(e));
    }

    SECINFO4resok resok;
    for (auto sec: sec_) {
        switch (sec) {
        case AUTH_NONE:
        case AUTH_SYS:
            resok.emplace_back(secinfo4(sec));
            break;
        case RPCSEC_GSS_KRB5:
            resok.emplace_back(
                secinfo4(
                    RPCSEC_GSS,
                    rpcsec_gss_info{
                        {0x2a,0x86,0x48,0x86,0xf7,0x12,0x01,0x02,0x02},
                        0, RPC_GSS_SVC_NONE }));
            break;
        case RPCSEC_GSS_KRB5I:
            resok.emplace_back(
                secinfo4(
                    RPCSEC_GSS,
                    rpcsec_gss_info{
                        {0x2a,0x86,0x48,0x86,0xf7,0x12,0x01,0x02,0x02},
                        0, RPC_GSS_SVC_INTEGRITY }));
            break;
        case RPCSEC_GSS_KRB5P:
            resok.emplace_back(
                secinfo4(
                    RPCSEC_GSS,
                    rpcsec_gss_info{
                        {0x2a,0x86,0x48,0x86,0xf7,0x12,0x01,0x02,0x02},
                        0, RPC_GSS_SVC_PRIVACY }));
            break;
        }
    }
    return SECINFO4res(NFS4_OK, move(resok));
}

SETATTR4res NfsServer::setattr(
    CompoundState& state,
    const SETATTR4args& args)
{
    auto& cred = CallContext::current().cred();
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::setattr("
                << args.stateid << ", ...)";
    if (!state.curr.file)
        return SETATTR4res{NFS4ERR_NOFILEHANDLE};
    try {
        if (args.stateid != STATEID_ANON) {
            // Make sure the stateid is valid
            state.session->client()->findState(state, args.stateid);
        }
        // XXX lookup stateid
        bitmap4 attrsset;
        state.curr.file->setattr(
            cred, importAttr(args.obj_attributes, attrsset));
        return SETATTR4res{NFS4_OK, move(attrsset)};
    }
    catch (nfsstat4 status) {
        return SETATTR4res{status};
    }
    catch (system_error& e) {
        return SETATTR4res{exportStatus(e)};
    }
}

SETCLIENTID4res NfsServer::setclientid(
    CompoundState& state,
    const SETCLIENTID4args& args)
{
    return SETCLIENTID4res(NFS4ERR_NOTSUPP);
}

SETCLIENTID_CONFIRM4res NfsServer::setclientid_confirm(
    CompoundState& state,
    const SETCLIENTID_CONFIRM4args& args)
{
    return SETCLIENTID_CONFIRM4res{NFS4ERR_NOTSUPP};
}

VERIFY4res NfsServer::verify(
    CompoundState& state,
    const VERIFY4args& args)
{
    VLOG(1) << "NfsServer::verify(...)";
    if (!state.curr.file)
        return VERIFY4res{NFS4ERR_NOFILEHANDLE};
    auto status = verifyAttr(state.curr.file, args.obj_attributes);
    if (status == NFS4ERR_SAME)
        return VERIFY4res{NFS4_OK};
    else
        return VERIFY4res{status};
}

WRITE4res NfsServer::write(
    CompoundState& state,
    const WRITE4args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::write("
                << args.stateid
                << ", " << args.offset
                << ", " << args.stable << ", ...)";

    if (!state.curr.file)
        return WRITE4res(NFS4ERR_NOFILEHANDLE);

    if (inGracePeriod())
        return WRITE4res(NFS4ERR_GRACE);

    auto client = state.session->client();
    try {
        shared_ptr<OpenFile> of;
        if (args.stateid == STATEID_ANON) {
            auto& cred = CallContext::current().cred();
            auto fs = findState(state.curr.file);
            if (fs->deny() & OPEN4_SHARE_ACCESS_WRITE)
                return WRITE4res(NFS4ERR_PERM);
            of = state.curr.file->open(cred, OpenFlags::WRITE);
        }
        else {
            auto ns = client->findState(state, args.stateid);
            if ((ns->access() & OPEN4_SHARE_ACCESS_WRITE) == 0)
                return WRITE4res(NFS4ERR_OPENMODE);
            of = ns->of();
        }
        auto n = of->write(args.offset, args.data);
        stable_how4 stable = UNSTABLE4;
        if (args.stable > UNSTABLE4) {
            of->flush();
            stable = FILE_SYNC4;
        }
        return WRITE4res(NFS4_OK, WRITE4resok{n, stable, writeverf_});
    }
    catch (nfsstat4 status) {
        return WRITE4res(status);
    }
    catch (system_error& e) {
        return WRITE4res(exportStatus(e));
    }
}

RELEASE_LOCKOWNER4res NfsServer::release_lockowner(
    CompoundState& state,
    const RELEASE_LOCKOWNER4args& args)
{
    return RELEASE_LOCKOWNER4res{NFS4ERR_NOTSUPP};
}

BACKCHANNEL_CTL4res NfsServer::backchannel_ctl(
    CompoundState& state,
    const BACKCHANNEL_CTL4args& args)
{
    VLOG(1) << "NfsServer::backchannel_ctl("
            << args.bca_cb_program << ", ...)";
    state.session->setCallback(args.bca_cb_program, args.bca_sec_parms);
    return BACKCHANNEL_CTL4res{NFS4_OK};
}

BIND_CONN_TO_SESSION4res NfsServer::bind_conn_to_session(
    CompoundState& state,
    const BIND_CONN_TO_SESSION4args& args)
{
    VLOG(1) << "NfsServer::bind_conn_to_session("
            << args.bctsa_sessid
            << ", " << args.bctsa_dir
            << ", " << args.bctsa_use_conn_in_rdma_mode << ")";
    auto it = sessionsById_.find(args.bctsa_sessid);
    if (it == sessionsById_.end())
        return BIND_CONN_TO_SESSION4res(NFS4ERR_BADSESSION);
    auto chan = CallContext::current().channel();
    it->second->addChannel(
        chan, args.bctsa_dir, args.bctsa_use_conn_in_rdma_mode);

    return BIND_CONN_TO_SESSION4res(
        NFS4_OK,
        BIND_CONN_TO_SESSION4resok{
            args.bctsa_sessid,
            channel_dir_from_server4(args.bctsa_dir & CDFS4_BOTH),
            false});
}

EXCHANGE_ID4res NfsServer::exchange_id(
    CompoundState& state,
    const EXCHANGE_ID4args& args)
{
    if (VLOG_IS_ON(1))
        VLOG(1) << "NfsServer::exchange_id("
                << args.eia_clientowner
                << ", " << args.eia_flags << ", ...)";
retry:
    unique_lock<mutex> lock(mutex_);
    auto& ctx = CallContext::current();
    auto& owner = args.eia_clientowner;
    shared_ptr<NfsClient> client;
    auto range = clientsByOwnerId_.equal_range(owner.co_ownerid);

    if (args.eia_state_protect.spa_how != SP4_NONE) {
        // The client MUST send the EXCHANGE_ID request with
        // RPCSEC_GSS and a service of RPC_GSS_SVC_INTEGRITY or
        // RPC_GSS_SVC_PRIVACY
        if (ctx.flavor() != RPCSEC_GSS_KRB5I &&
            ctx.flavor() != RPCSEC_GSS_KRB5P) {
            return EXCHANGE_ID4res(NFS4ERR_INVAL);
        }
    }

    if (args.eia_state_protect.spa_how == SP4_SSV) {
        return EXCHANGE_ID4res(NFS4ERR_NOTSUPP);
    }

    if (!(args.eia_flags & EXCHGID4_FLAG_UPD_CONFIRMED_REC_A)) {
        if (range.first == clientsByOwnerId_.end()) {
            // Case 1: New Owner ID
            VLOG(1) << "New client: " << args.eia_clientowner;
            auto id = nextClientId_++;
            client = make_shared<NfsClient>(
                id, args.eia_clientowner, ctx.principal(), leaseExpiry(),
                args.eia_state_protect);
            clientsById_[id] = client;
            clientsByOwnerId_.insert(
                make_pair(args.eia_clientowner.co_ownerid, client));
        }
        else {
            // We have a match by ownerid - see if we have a confirmed
            // client. We can have at most one confirmed client for any
            // given ownerid.
            for (auto it = range.first; it != range.second; ++it) {
                if (it->second->confirmed()) {
                    client = it->second;
                    if (client->owner() == args.eia_clientowner) {
                        // Case 2: Non-Update on Existing Client ID
                        VLOG(1) << "Existing confirmed client: "
                                << args.eia_clientowner;
                    }
                    else if (client->principal() != ctx.principal()) {
                        // Case 3: Client Collision
                        //
                        // If the old client has no state, just delete
                        // it and retry
                        VLOG(1) << "Client collision: "
                                << args.eia_clientowner;
                        if (!client->hasState()) {
                            for (auto it = range.first; it != range.second;
                                 ++it) {
                                clientsById_.erase(it->second->id());
                            }
                            clientsByOwnerId_.erase(range.first, range.second);
                            goto retry;
                        }
                        return EXCHANGE_ID4res(NFS4ERR_CLID_INUSE);
                    }
                    else {
                        // Case 5: Client Restart
                        //
                        // Delete any existing unconfirmed client
                        // record - there can be at most one of these
                        VLOG(1) << "Client restart: "
                                << args.eia_clientowner;
                        for (auto it = range.first; it != range.second;
                             ++it) {
                            if (!it->second->confirmed()) {
                                clientsById_.erase(it->second->id());
                                clientsByOwnerId_.erase(it);
                                break;
                            }
                        }

                        // Add a new unconfirmed record alongside the
                        // old confirmed one. If the client
                        // successfully creates a session, we will
                        // delete the old client and release its state
                        auto id = nextClientId_++;
                        client = make_shared<NfsClient>(
                            id, args.eia_clientowner, ctx.principal(),
                            leaseExpiry(), args.eia_state_protect);
                        clientsById_[id] = client;
                        clientsByOwnerId_.insert(
                            make_pair(args.eia_clientowner.co_ownerid, client));
                    }
                    break;
                }
            }
            if (!client) {
                // Case 4: Replacement of Unconfirmed Record
                VLOG(1) << "Replacing unconfirmed client: "
                        << args.eia_clientowner;
                for (auto it = range.first; it != range.second;
                     ++it) {
                    clientsById_.erase(it->second->id());
                }
                clientsByOwnerId_.erase(range.first, range.second);
                goto retry;
            }
        }
    }
    else {
        if (range.first != clientsByOwnerId_.end()) {
            for (auto it = range.first; it != range.second; ++it) {
                if (it->second->confirmed()) {
                    client = it->second;
                    if (client->owner() == args.eia_clientowner) {
                        // Case 6: Update
                        // XXX: perform update here
                        VLOG(1) << "Client update: "
                                << args.eia_clientowner;
                    }
                    else {
                        // Case 8: Update but Wrong Verifier
                        LOG(ERROR) << "Bad client verifier in update: "
                                   << args.eia_clientowner;
                        return EXCHANGE_ID4res(NFS4ERR_NOT_SAME);
                    }
                    break;
                }
            }
            if (!client) {
                // Case 7: Update but No Confirmed Record
                LOG(ERROR) << "Unknown client for update: "
                           << args.eia_clientowner;
                return EXCHANGE_ID4res(NFS4ERR_NOENT);
            }
        }
    }
    assert(client);

    state_protect4_r spr;
    if (args.eia_state_protect.spa_how == SP4_NONE) {
        spr = state_protect4_r(SP4_NONE);
    }
    else {
        // XXX: check spa_mach_ops
        auto ops = args.eia_state_protect.spa_mach_ops();
        spr = state_protect4_r(SP4_MACH_CRED, move(ops));
    }

    // Figure out flags to return
    uint32_t flags = EXCHGID4_FLAG_USE_NON_PNFS;
    auto fs = FilesystemManager::instance().begin()->second;
    if (fs->isData() && (flags & EXCHGID4_FLAG_USE_PNFS_DS)) {
        flags = EXCHGID4_FLAG_USE_PNFS_DS;
    } else if (fs->isMetadata() && (flags & EXCHGID4_FLAG_USE_PNFS_MDS)) {
        flags = EXCHGID4_FLAG_USE_PNFS_MDS;
    }

    VLOG(1) << "Returning clientid: " << hex << client->id();
    return EXCHANGE_ID4res(
        NFS4_OK,
        EXCHANGE_ID4resok{
            client->id(),
            client->sequence() + 1,
            flags,
            move(spr),
            owner_,
            {},         // server scope
            {}          // server impl id
        });
}

CREATE_SESSION4res NfsServer::create_session(
    CompoundState& state,
    const CREATE_SESSION4args& args)
{
    VLOG(1) << "NfsServer::create_session("
            << hex << args.csa_clientid
            << ", " << args.csa_sequence
            << ", " << args.csa_flags << ", ...)";

    unique_lock<mutex> lock(mutex_);
    auto& ctx = CallContext::current();

    auto it = clientsById_.find(args.csa_clientid);
    if (it == clientsById_.end()) {
        LOG(ERROR) << "Unknown client: " << hex << args.csa_clientid;
        return CREATE_SESSION4res(NFS4ERR_STALE_CLIENTID);
    }
    auto client = it->second;

    if (args.csa_sequence == client->sequence()) {
        auto& reply = client->reply();
        if (reply.csr_status == NFS4_OK) {
            auto resok = reply.csr_resok4();
            return CREATE_SESSION4res(NFS4_OK, move(resok));
        }
        else {
            return CREATE_SESSION4res(reply.csr_status);
        }
    }

    if (args.csa_sequence != client->sequence() + 1)
        return CREATE_SESSION4res(NFS4ERR_SEQ_MISORDERED);

    if (client->principal() != ctx.principal())
        return CREATE_SESSION4res(NFS4ERR_CLID_INUSE);

    if (!client->confirmed()) {
        // First purge any state associated with any old confirmed
        // client with matching ownerid
        auto range = clientsByOwnerId_.equal_range(client->owner().co_ownerid);
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second->confirmed()) {
                VLOG(1) << "Purging state for old clientid: "
                        << hex << it->second->id();
                it->second->releaseState();
                clientsById_.erase(it->second->id());
                clientsByOwnerId_.erase(it);
                break;
            }
        }
        VLOG(1) << "Confirming new clientid: " << hex << client->id();
        client->setConfirmed();
    }

    auto flags = args.csa_flags & CREATE_SESSION4_FLAG_CONN_BACK_CHAN;
    auto fca = args.csa_fore_chan_attrs;
    fca.ca_headerpadsize = 0;
    if (fca.ca_maxrequestsize > FLAGS_iosize + 512)
        fca.ca_maxrequestsize = FLAGS_iosize + 512;
    if (fca.ca_maxresponsesize > FLAGS_iosize + 512)
        fca.ca_maxresponsesize = FLAGS_iosize + 512;
    if (fca.ca_maxresponsesize_cached > FLAGS_iosize + 512)
        fca.ca_maxresponsesize_cached = FLAGS_iosize + 512;
    if (fca.ca_maxrequests > 8*thread::hardware_concurrency())
        fca.ca_maxrequests = 8*thread::hardware_concurrency();
    auto bca = args.csa_back_chan_attrs;
    bca.ca_headerpadsize = 0;

    ctx.channel()->setCloseOnIdle(false);
    ctx.channel()->setBufferSize(fca.ca_maxresponsesize);
    auto session = make_shared<NfsSession>(
        client, ctx.channel(), fca, bca,
        args.csa_cb_program, args.csa_sec_parms);
    sessionsById_[session->id()] = session;
    client->addSession(session);

    CREATE_SESSION4res res(
        NFS4_OK,
        CREATE_SESSION4resok{
            session->id(), client->sequence() + 1, flags, fca, bca});
    client->setReply(res);
    return res;
}

DESTROY_SESSION4res NfsServer::destroy_session(
    CompoundState& state,
    const DESTROY_SESSION4args& args)
{
    VLOG(1) << "NfsServer::destroy_session("
            << args.dsa_sessionid << ")";

    unique_lock<mutex> lock(mutex_);
    auto it = sessionsById_.find(args.dsa_sessionid);
    if (it == sessionsById_.end()) {
        LOG(ERROR) << "Session not found: " << args.dsa_sessionid;
        return DESTROY_SESSION4res{NFS4ERR_BADSESSION};
    }
    auto session = it->second;

    // If the destroy_session appears in a compound starting with
    // sequence and the sequence specifies the session to be
    // destroyed, it must be the last op in the compound.
    //
    // Note: if the destroy_session is last in the compound, the
    // shared_ptr in our compound state record will hold onto the
    // memory for the session (and its reply cache) until the compound
    // completes.
    if (state.session == session && state.opindex != state.opcount - 1)
        return DESTROY_SESSION4res{NFS4ERR_BADSESSION};

    auto chan = CallContext::current().channel();
    if (!session->validChannel(chan))
        return DESTROY_SESSION4res{NFS4ERR_CONN_NOT_BOUND_TO_SESSION};
    auto client = session->client();
    client->removeSession(session);
    sessionsById_.erase(it);
    return DESTROY_SESSION4res{NFS4_OK};
}

FREE_STATEID4res NfsServer::free_stateid(
    CompoundState& state,
    const FREE_STATEID4args& args)
{
    VLOG(1) << "NfsServer::free_stateid(" << args.fsa_stateid << ")";
    auto client = state.session->client();
    try {
        auto ns = client->findState(state, args.fsa_stateid, true);
        client->clearState(args.fsa_stateid);
        checkState(state.curr.file);
        return FREE_STATEID4res{NFS4_OK};
    }
    catch (nfsstat4 stat) {
        return FREE_STATEID4res{stat};
    }
}

GET_DIR_DELEGATION4res NfsServer::get_dir_delegation(
    CompoundState& state,
    const GET_DIR_DELEGATION4args& args)
{
    return GET_DIR_DELEGATION4res(NFS4ERR_NOTSUPP);
}

GETDEVICEINFO4res NfsServer::getdeviceinfo(
    CompoundState& state,
    const GETDEVICEINFO4args& args)
{
    VLOG(1) << "NfsServer::getdeviceinfo("
            << args.gdia_device_id
            << ", " << args.gdia_layout_type
            << ", " << args.gdia_maxcount
            << ", {" << (args.gdia_notify_types.size() > 0 ?
                         args.gdia_notify_types[0] : 0)
            << (args.gdia_notify_types.size() > 1 ? ",...}" : "})");
    if (args.gdia_layout_type != LAYOUT4_FLEX_FILES)
        return GETDEVICEINFO4res(NFS4ERR_UNKNOWN_LAYOUTTYPE);
    try {
        auto fs = FilesystemManager::instance().begin()->second;
        uint64_t devid = importDeviceid(args.gdia_device_id);
        auto dev = fs->findDevice(devid);
        ff_device_addr4 ffaddr;
        for (auto& ai: dev->addresses()) {
            string nettype;
            if (ai.family == AF_INET6)
                nettype = "tcp6";
            else
                nettype = "tcp";
            ffaddr.ffda_netaddrs.emplace_back(netaddr4{nettype, ai.uaddr()});
        }
        ffaddr.ffda_versions.emplace_back(
            ff_device_versions4{
                3, 0, uint32_t(FLAGS_iosize), uint32_t(FLAGS_iosize), false});
        device_addr4 addr;
        addr.da_layout_type = LAYOUT4_FLEX_FILES;
        addr.da_addr_body.resize(oncrpc::XdrSizeof(ffaddr));
        oncrpc::XdrMemory xmaddr(
            addr.da_addr_body.data(), addr.da_addr_body.size());
        xdr(ffaddr, static_cast<oncrpc::XdrSink*>(&xmaddr));

        // Register the callbacks, if any.
        int mask = (args.gdia_notify_types.size() > 0 ?
                    args.gdia_notify_types[0] : 0);
        auto client = state.session->client();
        client->setDeviceNotify(dev, mask);

        return GETDEVICEINFO4res(
            NFS4_OK, GETDEVICEINFO4resok{move(addr), {}});
    }
    catch (system_error& e) {
        return GETDEVICEINFO4res(exportStatus(e));
    }
}

GETDEVICELIST4res NfsServer::getdevicelist(
    CompoundState& state,
    const GETDEVICELIST4args& args)
{
    VLOG(1) << "NfsServer::getdeviceinfo("
            << args.gdla_layout_type
            << ", " << args.gdla_maxdevices
            << ", " << args.gdla_cookie
            << ", ...)";
    if (!state.curr.file)
        return GETDEVICELIST4res(NFS4ERR_NOFILEHANDLE);
    if (args.gdla_layout_type != LAYOUT4_FLEX_FILES)
        return GETDEVICELIST4res(NFS4ERR_UNKNOWN_LAYOUTTYPE);
    if (args.gdla_maxdevices == 0)
        return GETDEVICELIST4res(NFS4ERR_INVAL);

    auto fs = state.curr.file->fs();
    uint64_t gen;
    auto list = fs->devices(gen);
    auto cookie = args.gdla_cookie;

    auto p = list.begin();
    if (cookie > 0) {
        // Decode cookie and compare to gen
        oncrpc::XdrMemory xm(args.gdla_cookieverf.data(), NFS4_VERIFIER_SIZE);
        uint64_t cookieverf;
        xdr(cookieverf, static_cast<oncrpc::XdrSource*>(&xm));
        if (cookieverf != gen) {
            // List has changed since the caller started its iteration
            return GETDEVICELIST4res(NFS4ERR_NOT_SAME);
        }
        if (args.gdla_cookie >= list.size())
            p = list.end();
        else
            p += args.gdla_cookie;
    }

    vector<deviceid4> res;
    for (count4 i = 0; i < args.gdla_maxdevices; i++) {
        if (p == list.end())
            break;
        res.push_back(exportDeviceid((*p)->id()));
        cookie++;
    }
    verifier4 verf;
    oncrpc::XdrMemory xm(verf.data(), NFS4_VERIFIER_SIZE);
    xdr(gen, static_cast<oncrpc::XdrSink*>(&xm));
    return GETDEVICELIST4res(
        NFS4_OK,
        GETDEVICELIST4resok{
            cookie, verf, res, p == list.end()});
}

LAYOUTCOMMIT4res NfsServer::layoutcommit(
    CompoundState& state,
    const LAYOUTCOMMIT4args& args)
{
    auto& cred = CallContext::current().cred();

    VLOG(1) << "NfsServer::layoutcommit("
            << args.loca_offset
            << ", " << args.loca_length
            << ", " << args.loca_reclaim
            << ", " << args.loca_stateid
            << ", " << (args.loca_last_write_offset.no_newoffset ?
                        to_string(args.loca_last_write_offset.no_offset()) :
                        "<none>")
            << ", ...)";

    if (!state.curr.file)
        return LAYOUTCOMMIT4res(NFS4ERR_NOFILEHANDLE);
    auto oldsize = state.curr.file->getattr()->size();
    state.curr.file->setattr(
        cred,
        [&args, oldsize](auto sattr) {
            if (args.loca_last_write_offset.no_newoffset) {
                if (args.loca_last_write_offset.no_offset() + 1 > oldsize) {
                    sattr->setSize(
                        args.loca_last_write_offset.no_offset() + 1);
                }
            }
            if (args.loca_time_modify.nt_timechanged) {
                sattr->setMtime(
                    importTime(args.loca_time_modify.nt_time()));
            }
        });
    auto newsize = state.curr.file->getattr()->size();
    return LAYOUTCOMMIT4res(
        NFS4_OK,
        LAYOUTCOMMIT4resok{
            oldsize != newsize ?
                newsize4(true, length4(newsize)) : newsize4(false)});
}

LAYOUTGET4res NfsServer::layoutget(
    CompoundState& state,
    const LAYOUTGET4args& args)
{
    auto& cred = CallContext::current().cred();

    VLOG(1) << "NfsServer::layoutget("
            << args.loga_signal_layout_avail
            << ", " << args.loga_layout_type
            << ", " << args.loga_iomode
            << ", " << args.loga_offset
            << ", " << args.loga_length
            << ", " << args.loga_minlength
            << ", " << args.loga_stateid
            << ", " << args.loga_maxcount << ")";

    if (!state.curr.file)
        return LAYOUTGET4res(NFS4ERR_NOFILEHANDLE);
    if (!state.curr.file->fs()->isMetadata())
        return LAYOUTGET4res(NFS4ERR_LAYOUTUNAVAILABLE);
    if (args.loga_layout_type != LAYOUT4_FLEX_FILES)
        return LAYOUTGET4res(NFS4ERR_UNKNOWN_LAYOUTTYPE);
    if (args.loga_length < args.loga_minlength) {
        LOG(ERROR) << "loga_length < loga_minlength";
        return LAYOUTGET4res(NFS4ERR_INVAL);
    }
    if (args.loga_minlength != NFS4_UINT64_MAX &&
        args.loga_minlength > (NFS4_UINT64_MAX - args.loga_offset)) {
        LOG(ERROR) << "loga_minlength too large: " << args.loga_minlength;
        return LAYOUTGET4res(NFS4ERR_INVAL);
    }
    if (args.loga_length != NFS4_UINT64_MAX &&
        args.loga_length > (NFS4_UINT64_MAX - args.loga_offset)) {
        LOG(ERROR) << "loga_length too large: " << args.loga_minlength;
        return LAYOUTGET4res(NFS4ERR_INVAL);
    }
    if (args.loga_iomode != LAYOUTIOMODE4_READ &&
        args.loga_iomode != LAYOUTIOMODE4_RW) {
        LOG(ERROR) << "loga_iomode invalid";
        return LAYOUTGET4res(NFS4ERR_INVAL);
    }

    try {
        auto client = state.session->client();
        auto ns = client->findState(state, args.loga_stateid);

        // First get a set of pieces covering the requested range
        vector<shared_ptr<Piece>> pieces;
        auto fileSize = state.curr.file->getattr()->size();
        auto offset = args.loga_offset;
        auto len = args.loga_length;

        for (;;) {
            try {
                VLOG(1) << "Getting layout segment for offset " << offset;
                auto piece = state.curr.file->data(
                    cred, offset, args.loga_iomode == LAYOUTIOMODE4_RW);
                pieces.push_back(piece);
                auto id = piece->id();
                if (id.size == 0) {
                    offset = NFS4_UINT64_MAX;
                    break;
                }
                else {
                    offset = id.offset + id.size;
                    if (id.size > len || offset >= fileSize)
                        break;
                    if (len != NFS4_UINT64_MAX)
                        len -= id.size;
                }
            }
            catch (system_error& e) {
                LOG(ERROR) << "Error getting layout segment: " << e.what();
                return LAYOUTGET4res(NFS4ERR_LAYOUTUNAVAILABLE);
            }
        }

        // The Linux flex files client won't use any layout segment
        // with length less than u64m. Unfortunately if the filesystem
        // has a piece size less than u64m, we cannot return a layout
        // segment of length u64m since otherwise the client will
        // attempt to write outside the segment bounds.
        if (args.loga_iomode == LAYOUTIOMODE4_RW &&
            offset != NFS4_UINT64_MAX &&
            (offset - args.loga_offset) < args.loga_length) {
            return LAYOUTGET4res(NFS4ERR_LAYOUTUNAVAILABLE);
        }

        // Take the server lock to serialise with open
        unique_lock<mutex> lock(mutex_);
        auto fs = findState(lock, state.curr.file);

        // Check for existing state conflicts
        if (args.loga_iomode == LAYOUTIOMODE4_READ) {
            // We can issue a read layout if there are no existing
            // write opens. Note that we should be safe from existing
            // delegations or layouts since they will have been
            // recalled before the open for args.loga_stateid.
            for (auto ns: fs->opens()) {
                if (ns->client() == client)
                    continue;
                if (ns->access() & OPEN4_SHARE_ACCESS_WRITE) {
                    VLOG(1) << "Denying read layout: "
                            << "existing write open";
                    return LAYOUTGET4res(NFS4ERR_LAYOUTUNAVAILABLE);
                }
            }
        }
        else {
            // We can issue a write layout if there are no existing
            // opens from other clients
            for (auto ns: fs->opens()) {
                if (ns->client() == client)
                    continue;
                VLOG(1) << "Denying write layout: "
                        << "existing open";
                return LAYOUTGET4res(NFS4ERR_LAYOUTUNAVAILABLE);
            }
        }

        vector<shared_ptr<Device>> devices;
        LAYOUTGET4resok resok;
        resok.logr_return_on_close = false;
        resok.logr_layout.resize(pieces.size());
        try {
            for (int i = 0; i < int(pieces.size()); i++) {
                auto piece = pieces[i];
                auto& layout = resok.logr_layout[i];
                layout.lo_offset = piece->id().offset;
                // Pretend the last segment extends to infinity to keep
                // the current linux flex file layout implementation happy
                auto id = piece->id();
                if (args.loga_iomode == LAYOUTIOMODE4_READ &&
                    id.offset + id.size >= fileSize) {
                    layout.lo_length = NFS4_UINT64_MAX;
                }
                else if (id.size == 0) {
                    layout.lo_length = NFS4_UINT64_MAX;
                }
                else {
                    layout.lo_length = id.size;
                }
                layout.lo_iomode = args.loga_iomode;
                ff_layout4 ffl;
                ffl.ffl_stripe_unit = 0;
                for (int j = 0; j < piece->mirrorCount(); j++) {
                    auto m = piece->mirror(cred, j);
                    devices.push_back(m.first);
                    ff_mirror4 ffm;
                    ffm.ffm_data_servers.resize(1);
                    auto& ffds = ffm.ffm_data_servers[0];
                    ffds.ffds_deviceid = exportDeviceid(m.first->id());
                    ffds.ffds_efficiency = 0;
                    ffds.ffds_stateid = STATEID_ANON;
                    auto nfh = exportFileHandle(m.second);
                    ffds.ffds_fh_vers.push_back(nfh);
                    ffds.ffds_user = toUtf8string("0");
                    ffds.ffds_group = toUtf8string("0");
                    ffl.ffl_mirrors.push_back(move(ffm));
                }
                ffl.ffl_flags = 0;
                ffl.ffl_stats_collect_hint = 0;

                layout.lo_content.loc_type = LAYOUT4_FLEX_FILES;
                layout.lo_content.loc_body.resize(oncrpc::XdrSizeof(ffl));
                oncrpc::XdrMemory xm(
                    layout.lo_content.loc_body.data(),
                    layout.lo_content.loc_body.size());
                xdr(ffl, static_cast<oncrpc::XdrSink*>(&xm));
            }
        } catch (system_error& e) {
            LOG(ERROR) << "Error getting layout devices: " << e.what();
            return LAYOUTGET4res(NFS4ERR_LAYOUTUNAVAILABLE);
        }

        // Now that we have finished calling anything which might raise an
        // exception, its safe to create the layout state object
        if (ns->type() != NfsState::LAYOUT) {
            // Client presented an open, delegation or lock stateid -
            // we create a new layout state.

            // If there is an existing layout for this client and if
            // so, just delete it.
            auto oldns = fs->findLayout(client);
            if (oldns) {
                client->clearState(oldns->id());
                fs->removeLayout(oldns);
            }

            auto newns = client->addLayout(fs, args.loga_iomode, devices);
            newns->setOffset(args.loga_offset);
            newns->setLength(args.loga_length);
            fs->addLayout(newns);

            ns = newns;
        }
        else {
            ns->updateLayout();
        }
        VLOG(1) << "Returning layout stateid: " << ns->id();
        resok.logr_stateid = ns->id();

        return LAYOUTGET4res(NFS4_OK, move(resok));
    }
    catch (nfsstat4 status) {
        return LAYOUTGET4res(status);
    }
    catch (system_error& e) {
        return LAYOUTGET4res(exportStatus(e));
    }
}

LAYOUTRETURN4res NfsServer::layoutreturn(
    CompoundState& state,
    const LAYOUTRETURN4args& args)
{
    VLOG(1) << "NfsServer::layoutreturn("
            << args.lora_reclaim
            << ", " << args.lora_layout_type
            << ", " << args.lora_iomode << ", ...)";

    if (args.lora_layout_type != LAYOUT4_FLEX_FILES)
        return LAYOUTRETURN4res(NFS4ERR_BADLAYOUT);

    try {
        auto client = state.session->client();
        switch (args.lora_layoutreturn.lr_returntype) {
        case LAYOUTRETURN4_FILE: {
            if (!state.curr.file)
                return LAYOUTRETURN4res(NFS4ERR_NOFILEHANDLE);
            auto& ret = args.lora_layoutreturn.lr_layout();
            auto ns = client->findState(state, ret.lrf_stateid);
            if (ns->type() != NfsState::LAYOUT) {
                LOG(INFO) << "Not a layout: " << ns->id();
                return LAYOUTRETURN4res(NFS4ERR_BAD_STATEID);
            }
            // XXX: validate offset and length, parse body
            ns->updateLayout();
            auto id = ns->id();
            client->clearState(ns->id());
            checkState(state.curr.file);
            return LAYOUTRETURN4res(
                NFS4_OK, layoutreturn_stateid(true, move(id)));
            break;
        }

        case LAYOUTRETURN4_FSID:
        case LAYOUTRETURN4_ALL:
            client->clearLayouts();
            return LAYOUTRETURN4res(
                NFS4_OK, layoutreturn_stateid(false));
            break;
        }
    }
    catch (nfsstat4 status) {
        return LAYOUTRETURN4res(status);
    }
}

SECINFO_NO_NAME4res NfsServer::secinfo_no_name(
    CompoundState& state,
    const SECINFO_NO_NAME4args& args)
{
    VLOG(1) << "NfsServer::secinfo_no_name(" << args << ")";
    if (!state.curr.file)
        return SECINFO_NO_NAME4res(NFS4ERR_NOFILEHANDLE);
    SECINFO4resok resok;
    for (auto sec: sec_) {
        switch (sec) {
        case AUTH_NONE:
        case AUTH_SYS:
            resok.emplace_back(secinfo4(sec));
            break;
        case RPCSEC_GSS_KRB5:
            resok.emplace_back(
                secinfo4(
                    RPCSEC_GSS,
                    rpcsec_gss_info{
                        {0x2a,0x86,0x48,0x86,0xf7,0x12,0x01,0x02,0x02},
                        0, RPC_GSS_SVC_NONE }));
            break;
        case RPCSEC_GSS_KRB5I:
            resok.emplace_back(
                secinfo4(
                    RPCSEC_GSS,
                    rpcsec_gss_info{
                        {0x2a,0x86,0x48,0x86,0xf7,0x12,0x01,0x02,0x02},
                        0, RPC_GSS_SVC_INTEGRITY }));
            break;
        case RPCSEC_GSS_KRB5P:
            resok.emplace_back(
                secinfo4(
                    RPCSEC_GSS,
                    rpcsec_gss_info{
                        {0x2a,0x86,0x48,0x86,0xf7,0x12,0x01,0x02,0x02},
                        0, RPC_GSS_SVC_PRIVACY }));
            break;
        }
    }
    return SECINFO_NO_NAME4res(NFS4_OK, move(resok));
}

SEQUENCE4res NfsServer::sequence(
    CompoundState& state,
    const SEQUENCE4args& args)
{
    // Normal sequence handling is elsewhere - this can only be called
    // if there is a sequence op not at the state of the compound
    return SEQUENCE4res(NFS4ERR_SEQUENCE_POS);
}

SET_SSV4res NfsServer::set_ssv(
    CompoundState& state,
    const SET_SSV4args& args)
{
    return SET_SSV4res(NFS4ERR_NOTSUPP);
}

TEST_STATEID4res NfsServer::test_stateid(
    CompoundState& state,
    const TEST_STATEID4args& args)
{
    VLOG(1) << "NfsServer::test_stateid(...)";
    auto client = state.session->client();
    vector<nfsstat4> res;
    for (auto& id: args.ts_stateids) {
        try {
            client->findState(state, id);
            res.push_back(NFS4_OK);
        }
        catch (nfsstat4 stat) {
            res.push_back(stat);
        }
    }
    return TEST_STATEID4res(NFS4_OK, TEST_STATEID4resok{res});
}

WANT_DELEGATION4res NfsServer::want_delegation(
    CompoundState& state,
    const WANT_DELEGATION4args& args)
{
    return WANT_DELEGATION4res(NFS4ERR_NOTSUPP);
}

DESTROY_CLIENTID4res NfsServer::destroy_clientid(
    CompoundState& state,
    const DESTROY_CLIENTID4args& args)
{
    VLOG(1) << "NfsServer::destroy_clientid("
            << hex << args.dca_clientid << ")";

    unique_lock<mutex> lock(mutex_);
    auto it = clientsById_.find(args.dca_clientid);
    if (it == clientsById_.end())
        return DESTROY_CLIENTID4res{NFS4ERR_STALE_CLIENTID};
    auto client = it->second;
    if (client->hasState() || client->sessionCount() > 0) {
        // Note: this also covers the case where destroy_clientid is
        // in a compound starting with sequence and that sequence
        // refers to a session of the clientid being destroyed.
        if (client->hasState())
            LOG(ERROR) << "Can't destroy client with state, clientid: "
                       << client->id();
        if (client->sessionCount() > 0)
            LOG(ERROR) << "Can't destroy client with sessions, clientid: "
                       << client->id();
        return DESTROY_CLIENTID4res{NFS4ERR_CLIENTID_BUSY};
    }
    destroyClient(lock, client);
    return DESTROY_CLIENTID4res{NFS4_OK};
}

RECLAIM_COMPLETE4res NfsServer::reclaim_complete(
    CompoundState& state,
    const RECLAIM_COMPLETE4args& args)
{
    VLOG(1) << "NfsServer::reclaim_complete(" << args.rca_one_fs << ")";
    return RECLAIM_COMPLETE4res{NFS4_OK};
}

void NfsServer::dispatch(oncrpc::CallContext&& ctx)
{
    switch (ctx.proc()) {
    case NFSPROC4_NULL:
        null();
        ctx.sendReply([](auto){});
        break;

    case NFSPROC4_COMPOUND:
        compound(move(ctx));
        break;
    }
}

static bool isSingleton(nfs_opnum4 op)
{
    switch (op) {
    case OP_EXCHANGE_ID:
    case OP_CREATE_SESSION:
    case OP_BIND_CONN_TO_SESSION:
    case OP_DESTROY_SESSION:
    case OP_DESTROY_CLIENTID:
        return true;
    default:
        return false;
    }
}

void NfsServer::compound(oncrpc::CallContext&& ctx)
{
    static map<int, string> flavors {
        { AUTH_NONE, "none" },
        { AUTH_SYS, "sys" },
        { RPCSEC_GSS_KRB5, "krb5" },
        { RPCSEC_GSS_KRB5I, "krb5i" },
        { RPCSEC_GSS_KRB5P, "krb5p" },
    };

    // Check the auth flavor is allowed
    auto flavor = ctx.flavor();
    bool validFlavor = false;
    for (auto sec: sec_) {
        if (sec == flavor) {
            validFlavor = true;
            break;
        }
    }
    if (!validFlavor) {
        auto p = flavors.find(flavor);
        string s = p == flavors.end() ? to_string(flavor) : p->second;
        LOG(ERROR) << "NfsServer: auth too weak: " << s;
        ctx.authError(AUTH_TOOWEAK);
        return;
    }

    ctx.sendReply(
        [this, &ctx](oncrpc::XdrSink* xresults)
        {
            ctx.getArgs(
                [this, xresults](oncrpc::XdrSource* xargs)
                {
                    string tag;
                    uint32_t minorversion;
                    int opcount;
                    xdr(tag, xargs);
                    xdr(minorversion, xargs);
                    xdr(opcount, xargs);
                    if (minorversion != 1) {
                        xdr(NFS4ERR_MINOR_VERS_MISMATCH, xresults);
                        xdr(tag, xresults);
                        xdr(0, xresults);
                        return;
                    }

                    CompoundState state;
                    state.opindex = 0;
                    state.opcount = opcount;

                    // The first opcode must be sequence or a valid
                    // singleton op
                    nfs_opnum4 op;
                    xdr(op, xargs);
                    bool validSingleton = isSingleton(op);
                    if (op != OP_SEQUENCE && !validSingleton) {
                        xdr(NFS4ERR_OP_NOT_IN_SESSION, xresults);
                        xdr(tag, xresults);
                        xdr(1, xresults);
                        xdr(op, xresults);
                        xdr(NFS4ERR_OP_NOT_IN_SESSION, xresults);
                        return;
                    }

                    // If the first op is not sequence, it must be a singleton
                    if (validSingleton) {
                        if (opcount != 1) {
                            xdr(NFS4ERR_NOT_ONLY_OP, xresults);
                            xdr(tag, xresults);
                            xdr(1, xresults);
                            xdr(op, xresults);
                            xdr(NFS4ERR_NOT_ONLY_OP, xresults);
                            return;
                        }

                        oncrpc::XdrWord* statusp =
                            xresults->writeInline<oncrpc::XdrWord>(
                                sizeof(oncrpc::XdrWord));
                        xdr(tag, xresults);
                        xdr(1, xresults);
                        xdr(op, xresults);
                        *statusp = dispatchop(op, state, xargs, xresults);
                        return;
                    }

                    SEQUENCE4args seqargs;
                    SEQUENCE4res seqres;
                    oncrpc::XdrSink* xreply = xresults;
                    xdr(seqargs, xargs);

                    shared_ptr<NfsSession> session;
                    unique_lock<mutex> lock(mutex_);
                    auto p = sessionsById_.find(seqargs.sa_sessionid);
                    lock.unlock();
                    if (p != sessionsById_.end()) {
                        session = p->second;
                        seqres = session->sequence(state, seqargs);
                    }
                    else {
                        LOG(ERROR) << "Request received for unknown session: "
                                   << seqargs.sa_sessionid;
                        seqres = SEQUENCE4res(NFS4ERR_BADSESSION);
                    }
                    if (state.session) {
                        state.session->client()->setExpiry(leaseExpiry());
                    }
                    if (state.slot) {
                        if (seqargs.sa_sequenceid == state.slot->sequence) {
                            state.slot->reply->copyTo(xresults);
                            return;
                        }
                        state.slot->sequence++;
                        auto& ctx = CallContext::current();
                        state.slot->reply = make_unique<oncrpc::Message>(
                            ctx.channel()->bufferSize());
                        xreply = state.slot->reply.get();
                    }

                    oncrpc::XdrWord* statusp =
                        xreply->writeInline<oncrpc::XdrWord>(
                            sizeof(oncrpc::XdrWord));
                    assert(statusp != nullptr);
                    *statusp = seqres.sr_status;
                    xdr(tag, xreply);
                    oncrpc::XdrWord* opcountp =
                        xreply->writeInline<oncrpc::XdrWord>(
                            sizeof(oncrpc::XdrWord));
                    assert(opcountp != nullptr);
                    *opcountp = 1;
                    xdr(OP_SEQUENCE, xreply);
                    xdr(seqres, xreply);
                    if (seqres.sr_status == NFS4_OK) {
                        for (int i = 1; i < opcount; i++) {
                            nfs_opnum4 op;
                            nfsstat4 stat;

                            xdr(op, xargs);
                            *opcountp = i + 1;
                            xdr(op, xreply);

                            if (op == OP_SEQUENCE) {
                                stat = NFS4ERR_SEQUENCE_POS;
                                xdr(stat, xreply);
                                break;
                            }
                            state.opindex = i;
                            stat = dispatchop(op, state, xargs, xreply);
                            if (stat != NFS4_OK) {
                                *statusp = stat;
                                break;
                            }
                        }
                    }
                    if (xreply != xresults) {
                        xreply->flush();
                        state.slot->busy = false;
                        state.slot->reply->copyTo(xresults);
                    }
                });
        });
}

nfsstat4 NfsServer::dispatchop(
    nfs_opnum4 op, CompoundState& state,
    oncrpc::XdrSource* xargs, oncrpc::XdrSink* xresults)
{
#define OP2(OPNAME, METHOD, STATUS)             \
    case OP_##OPNAME: {                         \
        OPNAME##4args args;                     \
        try {                                   \
            xdr(args, xargs);                   \
        }                                       \
        catch (XdrError& e) {                   \
            xdr(NFS4ERR_BADXDR, xresults);      \
            return NFS4ERR_BADXDR;              \
        }                                       \
        OPNAME##4res res = METHOD(state, args); \
        xdr(res, xresults);                     \
        return res.STATUS;                      \
    }

#define OP(OPNAME, METHOD) OP2(OPNAME, METHOD, status);

#define OPnoargs(OPNAME, METHOD)                \
    case OP_##OPNAME: {                         \
        OPNAME##4res res = METHOD(state);       \
        xdr(res, xresults);                     \
        return res.status;                      \
    }

    switch (op) {
        OP(ACCESS, access);
        OP(CLOSE, close);
        OP(COMMIT, commit);
        OP(CREATE, create);
        OP(DELEGPURGE, delegpurge);
        OP(DELEGRETURN, delegreturn);
        OP(GETATTR, getattr);
        OPnoargs(GETFH, getfh);
        OP(LINK, link);
        OP(LOCK, lock);
        OP(LOCKT, lockt);
        OP(LOCKU, locku);
        OP(LOOKUP, lookup);
        OPnoargs(LOOKUPP, lookupp);
        OP(NVERIFY, nverify);
        OP(OPEN, open);
        OP(OPENATTR, openattr);
        OP(OPEN_CONFIRM, open_confirm);
        OP(OPEN_DOWNGRADE, open_downgrade);
        OP(PUTFH, putfh);
        OPnoargs(PUTPUBFH, putpubfh);
        OPnoargs(PUTROOTFH, putrootfh);
        OP(READ, read);
        OP(READDIR, readdir);
        OPnoargs(READLINK, readlink);
        OP(REMOVE, remove);
        OP(RENAME, rename);
        OP(RENEW, renew);
        OPnoargs(RESTOREFH, restorefh);
        OPnoargs(SAVEFH, savefh);
        OP(SECINFO, secinfo);
        OP(SETATTR, setattr);
        OP(SETCLIENTID, setclientid);
        OP(SETCLIENTID_CONFIRM, setclientid_confirm);
        OP(VERIFY, verify);
        OP(WRITE, write);
        OP(RELEASE_LOCKOWNER, release_lockowner);
        OP2(BACKCHANNEL_CTL, backchannel_ctl, bcr_status);
        OP2(BIND_CONN_TO_SESSION, bind_conn_to_session, bctsr_status);
        OP2(EXCHANGE_ID, exchange_id, eir_status);
        OP2(CREATE_SESSION, create_session, csr_status);
        OP2(DESTROY_SESSION, destroy_session, dsr_status);
        OP2(FREE_STATEID, free_stateid, fsr_status);
        OP2(GET_DIR_DELEGATION, get_dir_delegation, gddr_status);
        OP2(GETDEVICEINFO, getdeviceinfo, gdir_status);
        OP2(GETDEVICELIST, getdevicelist, gdlr_status);
        OP2(LAYOUTCOMMIT, layoutcommit, locr_status);
        OP2(LAYOUTGET, layoutget, logr_status);
        OP2(LAYOUTRETURN, layoutreturn, lorr_status);
        OP(SECINFO_NO_NAME, secinfo_no_name);
        OP2(SEQUENCE, sequence, sr_status);
        OP2(SET_SSV, set_ssv, ssr_status);
        OP2(TEST_STATEID, test_stateid, tsr_status);
        OP2(WANT_DELEGATION, want_delegation, wdr_status);
        OP2(DESTROY_CLIENTID, destroy_clientid, dcr_status);
        OP2(RECLAIM_COMPLETE, reclaim_complete, rcr_status);

    default:
        xdr(NFS4ERR_OP_ILLEGAL, xresults);
        return NFS4ERR_OP_ILLEGAL;
    }
#undef OP
}

detail::Clock::time_point NfsServer::leaseExpiry()
{
    // Add a few seconds to the reported lease_time value
    return clock_->now() + seconds(FLAGS_lease_time + 15);
}

int NfsServer::expireClients()
{
    unique_lock<mutex> lock(mutex_);
    auto now = clock_->now();
    int expired = 0;

    // Our expiry policy has several phases.
    //
    // 1: Any client which has not renewed its lease within the
    // lease_time and which has no state is purged.
    //
    // 2: An expired client which has no unrevoked state is purged
    // after 5*lease_time from its last renewal.
    //
    // 3: An expired client with unrevoked state is kept but if any
    // other client makes a request which conflicts with its state,
    // then that state entry is revoked.
    //
    // 3: A client which still has unrevoked state after 20*lease_time
    // from its last renewal has all its immediately revoked and is
    // purged.
restart:
    for (auto& e: clientsById_) {
        auto client = e.second;
        if (client->expiry() < now) {
            // Set the client's expired flag - this will be reset if
            // it renews
            client->setExpired();
            if (!client->hasState() && !client->hasRevokedState()) {
                LOG(INFO) << "Expiring client with no state, clientid: "
                          << hex << client->id();
                destroyClient(lock, client);
                expired++;
                goto restart;
            }
        }
        if (client->expiry() + seconds(4*FLAGS_lease_time) < now &&
            !client->hasState()) {
            LOG(INFO) << "Expiring client with revoked state, clientid: "
                      << hex << client->id();
            destroyClient(lock, client);
            expired++;
            goto restart;
        }
        if (client->expiry() + seconds(19*FLAGS_lease_time) < now) {
            LOG(INFO) << "Expiring client with unrevoked state, clientid: "
                      << hex << client->id();
            destroyClient(lock, client);
            expired++;
            goto restart;
        }
    }

    if (expired > 0) {
        // After expiry processing, it is possible that we may have
        // NfsFileState instances which can also be expired
        vector<shared_ptr<File>> toExpire;
        for (auto& e: files_)
            if (!e.second->hasState())
                toExpire.push_back(e.first);
        for (auto& f: toExpire)
            files_.erase(f);
    }

    return expired;
}

void NfsServer::destroyClient(
    unique_lock<mutex>& lock, shared_ptr<NfsClient> client)
{
    for (auto& session: client->sessions()) {
        sessionsById_.erase(session->id());
    }
    client->revokeState();
    auto range = clientsByOwnerId_.equal_range(client->owner().co_ownerid);
    clientsById_.erase(client->id());
    clientsByOwnerId_.erase(range.first, range.second);
}

function<void(Setattr*)> NfsServer::importAttr(
    const fattr4& attr, bitmap4& attrsset)
{
    using filesys::nfs4::set;
    return [&](auto sattr)
    {
        NfsAttr xattr;
        xattr.decode(attr);
        if (isset(attr.attrmask, FATTR4_MODE)) {
            set(attrsset, FATTR4_MODE);
            sattr->setMode(xattr.mode_);
        }
        if (isset(attr.attrmask, FATTR4_OWNER)) {
            set(attrsset, FATTR4_OWNER);
            sattr->setUid(idmapper_->toUid(toString(xattr.owner_)));
        }
        if (isset(attr.attrmask, FATTR4_OWNER_GROUP)) {
            set(attrsset, FATTR4_OWNER_GROUP);
            sattr->setGid(idmapper_->toUid(toString(xattr.owner_group_)));
        }
        if (isset(attr.attrmask, FATTR4_SIZE)) {
            set(attrsset, FATTR4_SIZE);
            sattr->setSize(xattr.size_);
        }
        if (isset(attr.attrmask, FATTR4_TIME_ACCESS_SET)) {
            set(attrsset, FATTR4_TIME_ACCESS_SET);
            switch (xattr.time_access_set_) {
            case SET_TO_SERVER_TIME4:
                sattr->setAtime(system_clock::now());
                break;
            case SET_TO_CLIENT_TIME4:
                sattr->setAtime(importTime(xattr.time_access_));
                break;
            }
        }
        if (isset(attr.attrmask, FATTR4_TIME_MODIFY_SET)) {
            set(attrsset, FATTR4_TIME_MODIFY_SET);
            switch (xattr.time_modify_set_) {
            case SET_TO_SERVER_TIME4:
                sattr->setMtime(system_clock::now());
                break;
            case SET_TO_CLIENT_TIME4:
                sattr->setMtime(importTime(xattr.time_modify_));
                break;
            }
        }
    };
}

static auto exportType(FileType type)
{
    switch (type) {
    case FileType::FILE:
        return NF4REG;
    case FileType::DIRECTORY:
        return NF4DIR;
    case FileType::BLOCKDEV:
        return NF4BLK;
    case FileType::CHARDEV:
        return NF4CHR;
    case FileType::SYMLINK:
        return NF4LNK;
    case FileType::SOCKET:
        return NF4SOCK;
    case FileType::FIFO:
        return NF4FIFO;
    }
    abort();
}

fattr4 NfsServer::exportAttr(shared_ptr<File> file, const bitmap4& wanted)
{
    using filesys::nfs4::set;
    auto& cred = CallContext::current().cred();
    NfsAttr xattr;
    auto attr = file->getattr();
    shared_ptr<Fsattr> fsattr;
    int i = 0;
    for (auto word: wanted) {
        while (word) {
            int j = firstSetBit(word);
            word ^= (1 << j);
            switch (i + j) {
            case FATTR4_SUPPORTED_ATTRS:
                set(xattr.supported_attrs_, FATTR4_SUPPORTED_ATTRS);
                set(xattr.supported_attrs_, FATTR4_CHANGE);
                set(xattr.supported_attrs_, FATTR4_FILEHANDLE);
                set(xattr.supported_attrs_, FATTR4_TYPE);
                set(xattr.supported_attrs_, FATTR4_FH_EXPIRE_TYPE);
                set(xattr.supported_attrs_, FATTR4_MODE);
                set(xattr.supported_attrs_, FATTR4_NUMLINKS);
                set(xattr.supported_attrs_, FATTR4_OWNER);
                set(xattr.supported_attrs_, FATTR4_OWNER_GROUP);
                set(xattr.supported_attrs_, FATTR4_SIZE);
                set(xattr.supported_attrs_, FATTR4_SPACE_USED);
                set(xattr.supported_attrs_, FATTR4_FSID);
                set(xattr.supported_attrs_, FATTR4_FILEID);
                set(xattr.supported_attrs_, FATTR4_TIME_ACCESS);
                set(xattr.supported_attrs_, FATTR4_TIME_CREATE);
                set(xattr.supported_attrs_, FATTR4_TIME_MODIFY);
                set(xattr.supported_attrs_, FATTR4_TIME_METADATA);
                set(xattr.supported_attrs_, FATTR4_FILES_AVAIL);
                set(xattr.supported_attrs_, FATTR4_FILES_FREE);
                set(xattr.supported_attrs_, FATTR4_FILES_TOTAL);
                set(xattr.supported_attrs_, FATTR4_SPACE_AVAIL);
                set(xattr.supported_attrs_, FATTR4_SPACE_FREE);
                set(xattr.supported_attrs_, FATTR4_SPACE_TOTAL);
                set(xattr.supported_attrs_, FATTR4_MAXREAD);
                set(xattr.supported_attrs_, FATTR4_MAXWRITE);
                set(xattr.supported_attrs_, FATTR4_LINK_SUPPORT);
                set(xattr.supported_attrs_, FATTR4_SYMLINK_SUPPORT);
                set(xattr.supported_attrs_, FATTR4_UNIQUE_HANDLES);
                set(xattr.supported_attrs_, FATTR4_NAMED_ATTR);
                set(xattr.supported_attrs_, FATTR4_LEASE_TIME);
                set(xattr.supported_attrs_, FATTR4_FS_LAYOUT_TYPES);
                set(xattr.supported_attrs_, FATTR4_LAYOUT_BLKSIZE);
                set(xattr.supported_attrs_, FATTR4_LAYOUT_ALIGNMENT);
                break;
            case FATTR4_CHANGE:
                xattr.change_ = attr->change();
                break;
            case FATTR4_FILEHANDLE:
                xattr.filehandle_ = exportFileHandle(file);
                break;
            case FATTR4_TYPE:
                xattr.type_ = exportType(attr->type());
                break;
            case FATTR4_FH_EXPIRE_TYPE:
                break;
            case FATTR4_MODE:
                xattr.mode_ = attr->mode();
                break;
            case FATTR4_NUMLINKS:
                xattr.numlinks_ = attr->nlink();
                break;
            case FATTR4_OWNER:
                xattr.owner_ =
                    toUtf8string(idmapper_->fromUid(attr->uid()));
                break;
            case FATTR4_OWNER_GROUP:
                xattr.owner_group_ =
                    toUtf8string(idmapper_->fromGid(attr->gid()));
                break;
            case FATTR4_SIZE:
                xattr.size_ = attr->size();
                break;
            case FATTR4_SPACE_USED:
                xattr.space_used_ = attr->used();
                break;
            case FATTR4_FSID:
                // XXX
                xattr.fsid_.major = 0;
                xattr.fsid_.minor = 0;
                break;
            case FATTR4_FILEID:
                xattr.fileid_ = attr->fileid();
                break;
            case FATTR4_TIME_ACCESS:
                xattr.time_access_ = exportTime(attr->atime());
                break;
            case FATTR4_TIME_CREATE:
                xattr.time_create_ = exportTime(attr->birthtime());
                break;
            case FATTR4_TIME_MODIFY:
                xattr.time_modify_ = exportTime(attr->mtime());
                break;
            case FATTR4_TIME_METADATA:
                xattr.time_metadata_ = exportTime(attr->ctime());
                break;
            case FATTR4_FILES_AVAIL:
                if (!fsattr)
                    fsattr = file->fsstat(cred);
                xattr.files_avail_ = fsattr->afiles();
                break;
            case FATTR4_FILES_FREE:
                if (!fsattr)
                    fsattr = file->fsstat(cred);
                xattr.files_free_ = fsattr->ffiles();
                break;
            case FATTR4_FILES_TOTAL:
                if (!fsattr)
                    fsattr = file->fsstat(cred);
                xattr.files_total_ = fsattr->tfiles();
                break;
            case FATTR4_SPACE_AVAIL:
                if (!fsattr)
                    fsattr = file->fsstat(cred);
                xattr.space_avail_ = fsattr->abytes();
                break;
            case FATTR4_SPACE_FREE:
                if (!fsattr)
                    fsattr = file->fsstat(cred);
                xattr.space_free_ = fsattr->fbytes();
                break;
            case FATTR4_SPACE_TOTAL:
                if (!fsattr)
                    fsattr = file->fsstat(cred);
                xattr.space_total_ = fsattr->tbytes();
                break;
            case FATTR4_MAXREAD:
                xattr.maxread_ = FLAGS_iosize;
                break;
            case FATTR4_MAXWRITE:
                xattr.maxwrite_ = FLAGS_iosize;
                break;
            case FATTR4_LINK_SUPPORT:
            case FATTR4_SYMLINK_SUPPORT:
            case FATTR4_UNIQUE_HANDLES:
                break;
            case FATTR4_LEASE_TIME:
                xattr.lease_time_ = FLAGS_lease_time;
                break;
            case FATTR4_FS_LAYOUT_TYPES: {
                if (file->fs()->isMetadata()) {
                    xattr.fs_layout_types_ = { LAYOUT4_FLEX_FILES };
                }
                else {
                    xattr.fs_layout_types_ = { };
                }
                break;
            }
            case FATTR4_LAYOUT_BLKSIZE:
                xattr.layout_blksize_ = attr->blockSize();
                //xattr.layout_blksize_ = FLAGS_iosize;
                break;
            case FATTR4_LAYOUT_ALIGNMENT:
                xattr.layout_alignment_ = attr->blockSize();
                //xattr.layout_alignment_ = FLAGS_iosize;
                break;
            default:
                // Don't set the bit in attrmask_
                continue;
            }
            set(xattr.attrmask_, i + j);
        }
        i += 32;
    }
    fattr4 res;
    xattr.encode(res);
    return res;
}

nfsstat4 NfsServer::verifyAttr(shared_ptr<File> file, const fattr4& check)
{
    using filesys::nfs4::set;
    NfsAttr xattr;
    xattr.decode(check);
    auto attr = file->getattr();
    int i = 0;
    for (auto word: check.attrmask) {
        while (word) {
            int j = firstSetBit(word);
            word ^= (1 << j);
            switch (i + j) {
            case FATTR4_SUPPORTED_ATTRS: {
                bitmap4 v;
                set(v, FATTR4_SUPPORTED_ATTRS);
                set(v, FATTR4_CHANGE);
                set(v, FATTR4_FILEHANDLE);
                set(v, FATTR4_TYPE);
                set(v, FATTR4_MODE);
                set(v, FATTR4_NUMLINKS);
                set(v, FATTR4_OWNER);
                set(v, FATTR4_OWNER_GROUP);
                set(v, FATTR4_SIZE);
                set(v, FATTR4_SPACE_USED);
                set(v, FATTR4_FSID);
                set(v, FATTR4_FILEID);
                set(v, FATTR4_TIME_ACCESS);
                set(v, FATTR4_TIME_CREATE);
                set(v, FATTR4_TIME_MODIFY);
                set(v, FATTR4_TIME_METADATA);
                if (v != xattr.supported_attrs_)
                    return NFS4ERR_NOT_SAME;
                break;
            }
            case FATTR4_CHANGE:
                if (xattr.change_ != attr->change())
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_FILEHANDLE:
                if (xattr.filehandle_ != exportFileHandle(file))
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_TYPE:
                if (xattr.type_ != exportType(attr->type()))
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_FH_EXPIRE_TYPE:
                if (xattr.fh_expire_type_ != FH4_PERSISTENT)
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_MODE:
                if (xattr.mode_ != attr->mode())
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_NUMLINKS:
                if (xattr.numlinks_ != attr->nlink())
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_OWNER:
                if (xattr.owner_ !=
                    toUtf8string(idmapper_->fromUid(attr->uid())))
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_OWNER_GROUP:
                if (xattr.owner_group_ !=
                    toUtf8string(idmapper_->fromGid(attr->gid())))
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_SIZE:
                if (xattr.size_ != attr->size())
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_SPACE_USED:
                if (xattr.space_used_ != attr->used())
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_FSID:
                // XXX
                if (xattr.fsid_.major != 0 || xattr.fsid_.minor != 0)
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_FILEID:
                if (xattr.fileid_ != attr->fileid())
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_TIME_ACCESS:
                if (xattr.time_access_ != exportTime(attr->atime()))
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_TIME_CREATE:
                if (xattr.time_create_ != exportTime(attr->birthtime()))
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_TIME_MODIFY:
                if (xattr.time_modify_ != exportTime(attr->mtime()))
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_TIME_METADATA:
                if (xattr.time_metadata_ != exportTime(attr->ctime()))
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_LEASE_TIME:
                if (xattr.lease_time_ != FLAGS_lease_time)
                    return NFS4ERR_NOT_SAME;
                break;
            case FATTR4_FS_LAYOUT_TYPES: {
                if (file->fs()->isMetadata()) {
                    if (xattr.fs_layout_types_.size() != 1 ||
                        xattr.fs_layout_types_[0] != LAYOUT4_FLEX_FILES)
                        return NFS4ERR_NOT_SAME;
                }
                else {
                    if (xattr.fs_layout_types_.size() != 0)
                        return NFS4ERR_NOT_SAME;
                }
                break;
            }
            case FATTR4_LAYOUT_BLKSIZE:
                if (xattr.layout_blksize_ != attr->blockSize())
                    return NFS4ERR_NOT_SAME;
            case FATTR4_LAYOUT_ALIGNMENT:
                if (xattr.layout_alignment_ != attr->blockSize())
                    return NFS4ERR_NOT_SAME;
            default:
                return NFS4ERR_INVAL;
            }
        }
        i += 32;
    }
    return NFS4ERR_SAME;
}

void NfsServer::setRecallHook(
    const filesys::nfs4::sessionid4& sessionid,
    std::function<void(const stateid4&, const nfs_fh4&)> hook)
{
    LOG(INFO) << "setting layoutrecall hook for " << sessionid;
    sessionsById_[sessionid]->setRecallHook(hook);
}

void NfsServer::setLayoutRecallHook(
    const filesys::nfs4::sessionid4& sessionid,
    std::function<void(filesys::nfs4::layouttype4 type,
                       filesys::nfs4::layoutiomode4 iomode,
                       bool changed,
                       const filesys::nfs4::layoutrecall4& recall)> hook)
{
    LOG(INFO) << "setting layoutrecall hook for " << sessionid;
    sessionsById_[sessionid]->setLayoutRecallHook(hook);
}
