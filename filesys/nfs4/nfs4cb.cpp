#include "nfs4fs.h"
#include "nfs4cb.h"

using namespace filesys::nfs4;
using namespace std;

NfsCallbackService::NfsCallbackService(NfsFilesystem* fs)
    : fs_(fs)
{
}

void NfsCallbackService::null()
{
    LOG(INFO) << "NfsCallbackService::null()";
}

CB_GETATTR4res NfsCallbackService::getattr(
    const CB_GETATTR4args& args)
{
    LOG(INFO) << "NfsCallbackService::getattr("
              << args.fh << ", ...)";
              auto file = fs_->find(args.fh);
    if (file) {
        auto deleg = file->delegation();
        if (deleg && deleg->isWrite()) {
            NfsAttr attr;
            set(attr.attrmask_, FATTR4_SIZE);
            set(attr.attrmask_, FATTR4_CHANGE);
            set(attr.attrmask_, FATTR4_TIME_MODIFY);
            attr.attrmask_ &= args.attr_request;
            attr.size_ = file->attr().size_;
            attr.change_ = file->attr().change_;
            attr.time_modify_ = file->attr().time_modify_;
            CB_GETATTR4resok resok;
            attr.encode(resok.obj_attributes);
            return CB_GETATTR4res(NFS4_OK, std::move(resok));
        }
        else {
            return CB_GETATTR4res(NFS4ERR_BADHANDLE);
        }
    }
    else {
        return CB_GETATTR4res(NFS4ERR_BADHANDLE);
    }
}

CB_RECALL4res NfsCallbackService::recall(
    const CB_RECALL4args& args)
{
    LOG(INFO) << "NfsCallbackService::recall("
              << args.stateid
              << ", " << args.truncate
              << ", " << args.fh << ")";

    auto file = fs_->find(args.fh);
    if (file) {
        auto deleg = file->delegation();
        if (deleg && deleg->stateid() == args.stateid) {
            std::thread t(
                [](auto file, auto truncate) {
                    if (truncate)
                        file->clearCache();
                    file->clearDelegation();
                },
                file, args.truncate);
            t.detach();
            return CB_RECALL4res{NFS4_OK};
        }
        else {
            return CB_RECALL4res{NFS4ERR_BAD_STATEID};
        }
    }
    else {
        return CB_RECALL4res{NFS4ERR_BADHANDLE};
    }
}

CB_LAYOUTRECALL4res NfsCallbackService::layoutrecall(
    const CB_LAYOUTRECALL4args& args)
{
    LOG(INFO) << "NfsCallbackService::layoutrecall()";
    return CB_LAYOUTRECALL4res{NFS4ERR_NOTSUPP};
}

CB_NOTIFY4res NfsCallbackService::notify(
    const CB_NOTIFY4args& args)
{
    LOG(INFO) << "NfsCallbackService::notify()";
    return CB_NOTIFY4res{NFS4ERR_NOTSUPP};
}

CB_PUSH_DELEG4res NfsCallbackService::push_deleg(
    const CB_PUSH_DELEG4args& args)
{
    LOG(INFO) << "NfsCallbackService::push_deleg()";
    return CB_PUSH_DELEG4res{NFS4ERR_NOTSUPP};
}

CB_RECALL_ANY4res NfsCallbackService::recall_any(
    const CB_RECALL_ANY4args& args)
{
    LOG(INFO) << "NfsCallbackService::recall_any()";
    return CB_RECALL_ANY4res{NFS4ERR_NOTSUPP};
}

CB_RECALLABLE_OBJ_AVAIL4res NfsCallbackService::recallable_obj_avail(
    const CB_RECALLABLE_OBJ_AVAIL4args& args)
{
    LOG(INFO) << "NfsCallbackService::recallable_obj_avail()";
    return CB_RECALLABLE_OBJ_AVAIL4res{NFS4ERR_NOTSUPP};
}

CB_RECALL_SLOT4res NfsCallbackService::recall_slot(
    const CB_RECALL_SLOT4args& args)
{
    LOG(INFO) << "NfsCallbackService::recall_slot()";
    return CB_RECALL_SLOT4res{NFS4ERR_NOTSUPP};
}

CB_WANTS_CANCELLED4res NfsCallbackService::wants_cancelled(
    const CB_WANTS_CANCELLED4args& args)
{
    LOG(INFO) << "NfsCallbackService::wants_cancelled()";
    return CB_WANTS_CANCELLED4res{NFS4ERR_NOTSUPP};
}

CB_NOTIFY_LOCK4res NfsCallbackService::notify_lock(
    const CB_NOTIFY_LOCK4args& args)
{
    LOG(INFO) << "NfsCallbackService::notify_lock()";
    return CB_NOTIFY_LOCK4res{NFS4ERR_NOTSUPP};
}

CB_NOTIFY_DEVICEID4res NfsCallbackService::notify_deviceid(
    const CB_NOTIFY_DEVICEID4args& args)
{
    LOG(INFO) << "NfsCallbackService::notify_deviceid()";
    return CB_NOTIFY_DEVICEID4res{NFS4ERR_NOTSUPP};
}

void NfsCallbackService::dispatch(oncrpc::CallContext&& ctx)
{
    switch (ctx.proc()) {
    case CB_NULL:
        null();
        ctx.sendReply([](auto){});
        break;

    case CB_COMPOUND:
        compound(move(ctx));
        break;
    }
}

void NfsCallbackService::compound(oncrpc::CallContext&& ctx)
{
    ctx.sendReply(
        [this, &ctx](oncrpc::XdrSink* xresults)
        {
            ctx.getArgs(
                [this, xresults](oncrpc::XdrSource* xargs)
                {
                    string tag;
                    uint32_t minorversion;
                    uint32_t callback_ident;
                    int opcount;
                    xdr(tag, xargs);
                    xdr(minorversion, xargs);
                    xdr(callback_ident, xargs);
                    xdr(opcount, xargs);
                    if (minorversion != 1) {
                        xdr(NFS4ERR_MINOR_VERS_MISMATCH, xresults);
                        xdr(tag, xresults);
                        xdr(0, xresults);
                        return;
                    }

                    // The first opcode must be sequence
                    nfs_cb_opnum4 op;
                    xdr(op, xargs);
                    if (op != OP_CB_SEQUENCE) {
                        xdr(NFS4ERR_OP_NOT_IN_SESSION, xresults);
                        xdr(tag, xresults);
                        xdr(1, xresults);
                        xdr(op, xresults);
                        xdr(NFS4ERR_OP_NOT_IN_SESSION, xresults);
                        return;
                    }
                    CB_SEQUENCE4args seqargs;
                    CB_SEQUENCE4res seqres;
                    Slot* slotp;
                    oncrpc::XdrSink* xreply = xresults;
                    xdr(seqargs, xargs);
                    slotp = slots_.data() + seqargs.csa_slotid;
                    if (seqargs.csa_sessionid != fs_->sessionid())
                        seqres = CB_SEQUENCE4res(NFS4ERR_BADSESSION);
                    else if (seqargs.csa_slotid >= slots_.size())
                        seqres = CB_SEQUENCE4res(NFS4ERR_BADSLOT);
                    else if (seqargs.csa_highest_slotid >= slots_.size())
                        seqres = CB_SEQUENCE4res(NFS4ERR_BAD_HIGH_SLOT);
                    else if (seqargs.csa_sequenceid < slotp->sequence ||
                        seqargs.csa_sequenceid > slotp->sequence + 1)
                        seqres = CB_SEQUENCE4res(NFS4ERR_SEQ_MISORDERED);
                    else if (seqargs.csa_sequenceid == slotp->sequence) {
                        slotp->reply->copyTo(xresults);
                        return;
                    }
                    else {
                        slotp->sequence++;
                        seqres = CB_SEQUENCE4res(
                            NFS4_OK,
                            CB_SEQUENCE4resok{
                                seqargs.csa_sessionid,
                                seqargs.csa_sequenceid,
                                seqargs.csa_slotid,
                                seqargs.csa_highest_slotid,
                                slotid4(slots_.size() - 1)});
                        slotp->reply = make_unique<oncrpc::Message>(8192);
                        xreply = slotp->reply.get();
                    }

                    oncrpc::XdrWord* statusp =
                        xreply->writeInline<oncrpc::XdrWord>(
                            sizeof(oncrpc::XdrWord));
                    assert(statusp != nullptr);
                    *statusp = seqres.csr_status;
                    xdr(tag, xreply);
                    oncrpc::XdrWord* opcountp =
                        xreply->writeInline<oncrpc::XdrWord>(
                            sizeof(oncrpc::XdrWord));
                    assert(opcountp != nullptr);
                    *opcountp = 1;
                    xdr(OP_CB_SEQUENCE, xreply);
                    xdr(seqres, xreply);
                    if (seqres.csr_status == NFS4_OK) {
                        for (int i = 1; i < opcount; i++) {
                            nfs_cb_opnum4 op;
                            nfsstat4 stat;

                            xdr(op, xargs);
                            *opcountp = i + 1;
                            xdr(op, xreply);

                            if (op == OP_CB_SEQUENCE) {
                                stat = NFS4ERR_SEQUENCE_POS;
                                xdr(stat, xreply);
                                break;
                            }
                            stat = dispatchop(op, xargs, xreply);
                            if (stat != NFS4_OK) {
                                *statusp = stat;
                                break;
                            }
                        }
                    }
                    if (xreply != xresults) {
                        slotp->reply->copyTo(xresults);
                    }
                });
        });
}

nfsstat4 NfsCallbackService::dispatchop(
    nfs_cb_opnum4 op, oncrpc::XdrSource* xargs, oncrpc::XdrSink* xresults)
{
#define OP(OPNAME, METHOD, STATUS)              \
    case OP_##OPNAME: {                     \
        OPNAME##4args args;                 \
        xdr(args, xargs);                   \
        OPNAME##4res res = METHOD(args);    \
        xdr(res, xresults);                 \
        return res.STATUS;                  \
    }

    switch (op) {
        OP(CB_GETATTR, getattr, status);
        OP(CB_RECALL, recall, status);
        OP(CB_LAYOUTRECALL, layoutrecall, clorr_status);
        OP(CB_NOTIFY, notify, cnr_status);
        OP(CB_PUSH_DELEG, push_deleg, cpdr_status);
        OP(CB_RECALL_ANY, recall_any, crar_status);
        OP(CB_RECALLABLE_OBJ_AVAIL, recallable_obj_avail, croa_status);
        OP(CB_RECALL_SLOT, recall_slot, rsr_status);
        OP(CB_WANTS_CANCELLED, wants_cancelled, cwcr_status);
        OP(CB_NOTIFY_LOCK, notify_lock, cnlr_status);
        OP(CB_NOTIFY_DEVICEID, notify_deviceid, cndr_status);

    default:
        xdr(NFS4ERR_OP_ILLEGAL, xresults);
        return NFS4ERR_OP_ILLEGAL;
    }
#undef OP
}

void NfsCallbackService::bind(shared_ptr<oncrpc::ServiceRegistry> svcreg)
{
    using placeholders::_1;
    svcreg->add(
        NFS4_CALLBACK, NFS_CB,
        std::bind(&NfsCallbackService::dispatch, this, _1));
}

void NfsCallbackService::unbind(shared_ptr<oncrpc::ServiceRegistry> svcreg)
{
    svcreg->remove(NFS4_CALLBACK, NFS_CB);
}
