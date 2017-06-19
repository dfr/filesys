/*-
 * Copyright (c) 2016-present Doug Rabson
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <glog/logging.h>

#include "nfs4proto.h"
#include "nfs4compound.h"

using namespace filesys::nfs4;

CompoundRequestEncoder::CompoundRequestEncoder(
    const std::string& tag, oncrpc::XdrSink* xdrs)
    : tag_(tag),
      xdrs_(xdrs)
{
    xdr(tag_, xdrs_);
    xdr(1, xdrs_);
    opcountp_ = xdrs_->writeInline<oncrpc::XdrWord>(
        sizeof(oncrpc::XdrWord));
    assert(opcountp_ != nullptr);
    opcount_ = 0;
}

CompoundRequestEncoder::~CompoundRequestEncoder()
{
    *opcountp_ = opcount_;
}

void CompoundRequestEncoder::add(nfs_opnum4 op)
{
    xdr(reinterpret_cast<uint32_t&>(op), xdrs_);
    opcount_++;
}

template <typename... Args>
void CompoundRequestEncoder::add(nfs_opnum4 op, const Args&... args)
{
    xdr(reinterpret_cast<uint32_t&>(op), xdrs_);
    encodeArgs(args...);
    opcount_++;
}

template <typename T, typename... Rest>
void CompoundRequestEncoder::encodeArgs(const T& arg, const Rest&... rest)
{
    xdr(arg, xdrs_);
    encodeArgs(rest...);
}

void CompoundRequestEncoder::access(uint32_t mode)
{
    add(OP_ACCESS, mode);
}

void CompoundRequestEncoder::close(seqid4 seqid, stateid4 open_stateid)
{
    add(OP_CLOSE, seqid, open_stateid);
}

void CompoundRequestEncoder::commit(offset4 offset, count4 count)
{
    add(OP_COMMIT, offset, count);
}

void CompoundRequestEncoder::create(
    const createtype4& objtype, const component4& objname,
    const fattr4& createttrs)
{
    add(OP_CREATE, objtype, objname, createttrs);
}

void CompoundRequestEncoder::delegpurge(clientid4 clientid)
{
    add(OP_DELEGPURGE, clientid);
}

void CompoundRequestEncoder::delegreturn(stateid4 deleg_stateid)
{
    add(OP_DELEGRETURN, deleg_stateid);
}

void CompoundRequestEncoder::getattr(const bitmap4& attr_request)
{
    add(OP_GETATTR, attr_request);
}

void CompoundRequestEncoder::getfh()
{
    add(OP_GETFH);
}

void CompoundRequestEncoder::link(const component4& newname)
{
    add(OP_LINK, newname);
}

void CompoundRequestEncoder::lock(
    nfs_lock_type4 locktype, bool reclaim, offset4 offset, length4 length,
    locker4 locker)
{
    add(OP_LOCK, locktype, reclaim, offset, length, locker);
}

void CompoundRequestEncoder::lockt(
    nfs_lock_type4 locktype, offset4 offset, length4 length,
    lock_owner4 owner)
{
    add(OP_LOCKT, locktype, offset, length, owner);
}

void CompoundRequestEncoder::locku(
    nfs_lock_type4 locktype, seqid4 seqid, stateid4 lock_stateid,
    offset4 offset, length4 length)
{
    add(OP_LOCKU, locktype, seqid, lock_stateid, offset, length);
}

void CompoundRequestEncoder::lookup(const component4& objname)
{
    add(OP_LOOKUP, objname);
}

void CompoundRequestEncoder::lookupp()
{
    add(OP_LOOKUPP);
}

void CompoundRequestEncoder::nverify(const fattr4& obj_attributes)
{
    add(OP_NVERIFY, obj_attributes);
}

void CompoundRequestEncoder::open(
    seqid4 seqid, uint32_t share_access, uint32_t share_deny,
    open_owner4 owner, openflag4 openhow, open_claim4 claim)
{
    add(OP_OPEN, seqid, share_access, share_deny, owner, openhow, claim);
}

void CompoundRequestEncoder::openattr(bool createdir)
{
    add(OP_OPENATTR, createdir);
}

void CompoundRequestEncoder::open_confirm(stateid4 open_stateid, seqid4 seqid)
{
    add(OP_OPEN_CONFIRM, open_stateid, seqid);
}

void CompoundRequestEncoder::open_downgrade(
    stateid4 open_stateid, seqid4 seqid, uint32_t share_access,
    uint32_t share_deny)
{
    add(OP_OPEN_DOWNGRADE, open_stateid, seqid, share_access, share_deny);
}

void CompoundRequestEncoder::putfh(const nfs_fh4& object)
{
    add(OP_PUTFH, object);
}

void CompoundRequestEncoder::putpubfh()
{
    add(OP_PUTPUBFH);
}

void CompoundRequestEncoder::putrootfh()
{
    add(OP_PUTROOTFH);
}

void CompoundRequestEncoder::read(stateid4 stateid, offset4 offset, count4 count)
{
    add(OP_READ, stateid, offset, count);
}

void CompoundRequestEncoder::readdir(
    nfs_cookie4 cookie, const verifier4& cookieverf, count4 dircount,
    count4 maxcount, const bitmap4& attr_request)
{
    add(OP_READDIR, cookie, cookieverf, dircount, maxcount, attr_request);
}

void CompoundRequestEncoder::readlink()
{
    add(OP_READLINK);
}

void CompoundRequestEncoder::remove(const component4& target)
{
    add(OP_REMOVE, target);
}

void CompoundRequestEncoder::rename(const component4& oldname, const component4& newname)
{
    add(OP_RENAME, oldname, newname);
}

void CompoundRequestEncoder::renew(clientid4 clientid)
{
    add(OP_RENEW, clientid);
}

void CompoundRequestEncoder::restorefh()
{
    add(OP_RESTOREFH);
}

void CompoundRequestEncoder::savefh()
{
    add(OP_SAVEFH);
}

void CompoundRequestEncoder::secinfo(const component4& name)
{
    add(OP_SECINFO, name);
}

void CompoundRequestEncoder::setattr(const stateid4& stateid, const fattr4& attrset)
{
    add(OP_SETATTR, stateid, attrset);
}

void CompoundRequestEncoder::setclientid(
    const nfs_client_id4& client, const cb_client4& callback,
    uint32_t callback_ident)
{
    add(OP_SETCLIENTID, client, callback, callback_ident);
}

void CompoundRequestEncoder::setclientid_confirm(
    clientid4 clientid, verifier4 setclientid_confirm)
{
    add(OP_SETCLIENTID_CONFIRM, clientid, setclientid_confirm);
}

void CompoundRequestEncoder::verify(const fattr4& obj_attributes)
{
    add(OP_VERIFY, obj_attributes);
}

void CompoundRequestEncoder::write(
    stateid4 stateid, offset4 offset, stable_how4 stable,
    std::shared_ptr<oncrpc::Buffer> data)
{
    add(OP_WRITE, stateid, offset, stable, data);
}

void CompoundRequestEncoder::release_lockowner(const lock_owner4& lock_owner)
{
    add(OP_RELEASE_LOCKOWNER, lock_owner);
}

void CompoundRequestEncoder::backchannel_ctl(
    uint32_t cb_program,
    const std::vector<callback_sec_parms4>& sec_parms)
{
    add(OP_BACKCHANNEL_CTL, cb_program, sec_parms);
}

void CompoundRequestEncoder::bind_conn_to_session(
    const sessionid4& sessid, channel_dir_from_client4 dir,
    bool use_conn_in_rdma_mode)
{
    add(OP_BIND_CONN_TO_SESSION, sessid, dir, use_conn_in_rdma_mode);
}

void CompoundRequestEncoder::exchange_id(
    const client_owner4& clientowner, uint32_t flags,
    state_protect4_a state_protect,
    const std::vector<nfs_impl_id4> client_impl_id)
{
    add(OP_EXCHANGE_ID, clientowner, flags, state_protect, client_impl_id);
}

void CompoundRequestEncoder::create_session(
    clientid4 clientid, sequenceid4 sequence, uint32_t flags,
    const channel_attrs4& fore_chan_attrs,
    const channel_attrs4& back_chan_attrs,
    uint32_t cb_program, const std::vector<callback_sec_parms4>& sec_parms)
{
    add(OP_CREATE_SESSION, clientid, sequence, flags, fore_chan_attrs,
        back_chan_attrs, cb_program, sec_parms);
}

void CompoundRequestEncoder::destroy_session(sessionid4 sessionid)
{
    add(OP_DESTROY_SESSION, sessionid);
}

void CompoundRequestEncoder::free_stateid(stateid4 stateid)
{
    add(OP_FREE_STATEID, stateid);
}

// get_dir_delegation
// getdeviceinfo
// getdevicelist

void CompoundRequestEncoder::layoutcommit(
    offset4 offset, length4 length, bool reclaim, const stateid4& stateid,
    const newoffset4& last_write_offset, const newtime4& time_modify,
    const layoutupdate4& layoutupdate)
{
    add(OP_LAYOUTCOMMIT, offset, length, reclaim, stateid,
        last_write_offset, time_modify, layoutupdate);
}

void CompoundRequestEncoder::layoutget(
    bool signal_layout_avail, layouttype4 layout_type,
    layoutiomode4 iomode, offset4 offset, length4 length,
    length4 minlength, const stateid4& stateid, count4 maxcount)
{
    add(OP_LAYOUTGET, signal_layout_avail, layout_type, iomode, offset,
        length, minlength, stateid, maxcount);
}

void CompoundRequestEncoder::layoutreturn(
    bool reclaim, layouttype4 layout_type, layoutiomode4 iomode,
    const layoutreturn4& layoutreturn)
{
    add(OP_LAYOUTRETURN, reclaim, layout_type, iomode, layoutreturn);
}

void CompoundRequestEncoder::sequence(
    const sessionid4& sessionid, sequenceid4 sequenceid, slotid4 slotid,
    slotid4 highest_slotid, bool cachethis)
{
    add(OP_SEQUENCE, sessionid, sequenceid, slotid, highest_slotid,
        cachethis);
}

void CompoundRequestEncoder::test_stateid(const std::vector<stateid4>& stateids)
{
    add(OP_TEST_STATEID, stateids);
}

// want_delegation

void CompoundRequestEncoder::destroy_clientid(clientid4 clientid)
{
    add(OP_DESTROY_CLIENTID, clientid);
}

void CompoundRequestEncoder::reclaim_complete(bool one_fs)
{
    add(OP_RECLAIM_COMPLETE, one_fs);
}

CompoundReplyDecoder::CompoundReplyDecoder(
    const std::string& tag, oncrpc::XdrSource* xdrs)
    : tag_(tag),
      xdrs_(xdrs)
{
    std::string rtag;
    xdr(status_, xdrs_);
    xdr(rtag, xdrs_);
    if (rtag != tag_) {
        LOG(FATAL) << "Unexpected tag \"" << rtag << "\" in COMPOUND4res";
    }
    xdr(count_, xdrs_);
}

nfsstat4 CompoundReplyDecoder::status(nfs_opnum4 op)
{
    if (opcount_ >= count_) {
        LOG(FATAL) << "No reply for op index: " << opcount_;
    }
    nfs_opnum4 rop;
    xdr(reinterpret_cast<uint32_t&>(rop), xdrs_);
    if (rop != op) {
        LOG(FATAL) << "Unexpected op in COMPOUND4res";
    }
    nfsstat4 status;
    xdr(status, xdrs_);
    opcount_++;

    return status;
}

void CompoundReplyDecoder::check(nfs_opnum4 op)
{
    auto st = status(op);
    if (st != NFS4_OK)
        throw st;
}

template <typename ResokType>
ResokType CompoundReplyDecoder::get(nfs_opnum4 op)
{
    check(op);
    ResokType resok;
    xdr(resok, xdrs_);
    return resok;
}

ACCESS4resok CompoundReplyDecoder::access()
{
    return get<ACCESS4resok>(OP_ACCESS);
}

stateid4 CompoundReplyDecoder::close()
{
    return get<stateid4>(OP_CLOSE);
}

COMMIT4resok CompoundReplyDecoder::commit()
{
    return get<COMMIT4resok>(OP_COMMIT);
}

CREATE4resok CompoundReplyDecoder::create()
{
    return get<CREATE4resok>(OP_CREATE);
}

void CompoundReplyDecoder::delegpurge()
{
    check(OP_DELEGPURGE);
}

void CompoundReplyDecoder::delegreturn()
{
    check(OP_DELEGRETURN);
}

GETATTR4resok CompoundReplyDecoder::getattr()
{
    return get<GETATTR4resok>(OP_GETATTR);
}

GETFH4resok CompoundReplyDecoder::getfh()
{
    return get<GETFH4resok>(OP_GETFH);
}

LINK4resok CompoundReplyDecoder::link()
{
    return get<LINK4resok>(OP_LINK);
}

LOCK4resok CompoundReplyDecoder::lock()
{
    return get<LOCK4resok>(OP_LOCK);
}

void CompoundReplyDecoder::lockt()
{
    auto st = status(OP_LOCKT);
    if (st == NFS4_OK)
        return;
    if (st == NFS4ERR_DENIED) {
        LOCK4denied denied;
        xdr(denied, xdrs_);
        throw denied;
    }
    throw st;
}

stateid4 CompoundReplyDecoder::locku()
{
    return get<stateid4>(OP_LOCKU);
}

void CompoundReplyDecoder::lookup()
{
    check(OP_LOOKUP);
}

void CompoundReplyDecoder::lookupp()
{
    check(OP_LOOKUPP);
}

nfsstat4 CompoundReplyDecoder::nverify()
{
    return status(OP_NVERIFY);
}

OPEN4resok CompoundReplyDecoder::open()
{
    return get<OPEN4resok>(OP_OPEN);
}

void CompoundReplyDecoder::openattr()
{
    check(OP_OPENATTR);
}

OPEN_CONFIRM4resok CompoundReplyDecoder::open_confirm()
{
    return get<OPEN_CONFIRM4resok>(OP_OPEN_CONFIRM);
}

OPEN_DOWNGRADE4resok CompoundReplyDecoder::open_downgrade()
{
    return get<OPEN_DOWNGRADE4resok>(OP_OPEN_DOWNGRADE);
}

void CompoundReplyDecoder::putfh()
{
    check(OP_PUTFH);
}

void CompoundReplyDecoder::putpubfh()
{
    check(OP_PUTPUBFH);
}

void CompoundReplyDecoder::putrootfh()
{
    check(OP_PUTROOTFH);
}

READ4resok CompoundReplyDecoder::read()
{
    return get<READ4resok>(OP_READ);
}

READDIR4resok CompoundReplyDecoder::readdir()
{
    return get<READDIR4resok>(OP_READDIR);
}

READLINK4resok CompoundReplyDecoder::readlink()
{
    return get<READLINK4resok>(OP_READLINK);
}

REMOVE4resok CompoundReplyDecoder::remove()
{
    return get<REMOVE4resok>(OP_REMOVE);
}

RENAME4resok CompoundReplyDecoder::rename()
{
    return get<RENAME4resok>(OP_RENAME);
}

void CompoundReplyDecoder::renew()
{
    check(OP_RENEW);
}

void CompoundReplyDecoder::restorefh()
{
    check(OP_RESTOREFH);
}

void CompoundReplyDecoder::savefh()
{
    check(OP_SAVEFH);
}

SECINFO4resok CompoundReplyDecoder::secinfo()
{
    return get<SECINFO4resok>(OP_SECINFO);
}

bitmap4 CompoundReplyDecoder::setattr()
{
    return get<bitmap4>(OP_SETATTR);
}

SETCLIENTID4resok CompoundReplyDecoder::setclientid()
{
    auto st = status(OP_SETCLIENTID);
    if (st == NFS4_OK) {
        SETCLIENTID4resok resok;
        xdr(resok, xdrs_);
        return resok;
    }
    else if (st == NFS4ERR_CLID_INUSE) {
        clientaddr4 client_using;
        xdr(client_using, xdrs_);
        LOG(FATAL) << "Client ID in use by " << client_using.na_r_netid
                   << ":" << client_using.na_r_addr;
    }
    throw st;
}

void CompoundReplyDecoder::setclientid_confirm()
{
    check(OP_SETCLIENTID_CONFIRM);
}

nfsstat4 CompoundReplyDecoder::verify()
{
    return status(OP_VERIFY);
}

WRITE4resok CompoundReplyDecoder::write()
{
    return get<WRITE4resok>(OP_WRITE);
}

void CompoundReplyDecoder::release_lockowner()
{
    check(OP_RELEASE_LOCKOWNER);
}

void CompoundReplyDecoder::backchannel_ctl()
{
    check(OP_BACKCHANNEL_CTL);
}

BIND_CONN_TO_SESSION4resok CompoundReplyDecoder::bind_conn_to_session()
{
    return get<BIND_CONN_TO_SESSION4resok>(OP_BIND_CONN_TO_SESSION);
}

EXCHANGE_ID4resok CompoundReplyDecoder::exchange_id()
{
    return get<EXCHANGE_ID4resok>(OP_EXCHANGE_ID);
}

CREATE_SESSION4resok CompoundReplyDecoder::create_session()
{
    return get<CREATE_SESSION4resok>(OP_CREATE_SESSION);
}

void CompoundReplyDecoder::destroy_session()
{
    check(OP_DESTROY_SESSION);
}

void CompoundReplyDecoder::free_stateid()
{
    check(OP_FREE_STATEID);
}

// get_dir_delegation
// getdeviceinfo
// getdevicelist

LAYOUTCOMMIT4resok CompoundReplyDecoder::layoutcommit()
{
    return get<LAYOUTCOMMIT4resok>(OP_LAYOUTCOMMIT);
}

LAYOUTGET4resok CompoundReplyDecoder::layoutget()
{
    return get<LAYOUTGET4resok>(OP_LAYOUTGET);
}

layoutreturn_stateid CompoundReplyDecoder::layoutreturn()
{
    return get<layoutreturn_stateid>(OP_LAYOUTRETURN);
}

SEQUENCE4resok CompoundReplyDecoder::sequence()
{
    return get<SEQUENCE4resok>(OP_SEQUENCE);
}

TEST_STATEID4resok CompoundReplyDecoder::test_stateid()
{
    return get<TEST_STATEID4resok>(OP_TEST_STATEID);
}

// want_delegation

void CompoundReplyDecoder::destroy_clientid()
{
    check(OP_DESTROY_CLIENTID);
}

void CompoundReplyDecoder::reclaim_complete()
{
    check(OP_RECLAIM_COMPLETE);
}

CallbackRequestEncoder::CallbackRequestEncoder(
    const std::string& tag, oncrpc::XdrSink* xdrs)
    : tag_(tag),
      xdrs_(xdrs)
{
    xdr(tag_, xdrs_);
    xdr(1, xdrs_);
    xdr(0, xdrs_);
    opcountp_ = xdrs_->writeInline<oncrpc::XdrWord>(
        sizeof(oncrpc::XdrWord));
    assert(opcountp_ != nullptr);
    opcount_ = 0;
}

CallbackRequestEncoder::~CallbackRequestEncoder()
{
    *opcountp_ = opcount_;
}

void CallbackRequestEncoder::add(nfs_cb_opnum4 op)
{
    xdr(reinterpret_cast<uint32_t&>(op), xdrs_);
    opcount_++;
}

template <typename... Args>
void CallbackRequestEncoder::add(nfs_cb_opnum4 op, const Args&... args)
{
    xdr(reinterpret_cast<uint32_t&>(op), xdrs_);
    encodeArgs(args...);
    opcount_++;
}

void encodeArgs()
{
}

template <typename T, typename... Rest>
void CallbackRequestEncoder::encodeArgs(const T& arg, const Rest&... rest)
{
    xdr(arg, xdrs_);
    encodeArgs(rest...);
}

void CallbackRequestEncoder::getattr(
    const nfs_fh4& fh, const bitmap4& attr_request)
{
    add(OP_CB_GETATTR, fh, attr_request);
}

void CallbackRequestEncoder::recall(
    const stateid4& stateid, bool truncate, const nfs_fh4& fh)
{
    add(OP_CB_RECALL, stateid, truncate, fh);
}

void CallbackRequestEncoder::layoutrecall(
    layouttype4 type, layoutiomode4 iomode, bool changed,
    const layoutrecall4& recall)
{
    add(OP_CB_LAYOUTRECALL, type, iomode, changed, recall);
}

void CallbackRequestEncoder::sequence(
    const sessionid4& session, sequenceid4 sequence,
    slotid4 slotid, slotid4 highest_slotid, bool cachethis,
    std::vector<referring_call_list4> referring_call_lists)
{
    add(OP_CB_SEQUENCE, session, sequence, slotid, highest_slotid,
        cachethis, referring_call_lists);
}

void CallbackRequestEncoder::notify_deviceid(
    const std::vector<notify4>& changes)
{
    add(OP_CB_NOTIFY_DEVICEID, changes);
}

void CallbackRequestEncoder::recall_any(
    uint32_t objects_to_keep, const bitmap4& type_mask)
{
    add(OP_CB_RECALL_ANY, objects_to_keep, type_mask);
}

CallbackReplyDecoder::CallbackReplyDecoder(
    const std::string& tag, oncrpc::XdrSource* xdrs)
    : tag_(tag),
      xdrs_(xdrs)
{
    std::string rtag;
    xdr(status_, xdrs_);
    xdr(rtag, xdrs_);
    if (rtag != tag_) {
        LOG(FATAL) << "Unexpected tag \"" << rtag << "\" in COMPOUND4res";
    }
    xdr(count_, xdrs_);
}

nfsstat4 CallbackReplyDecoder::status(nfs_cb_opnum4 op)
{
    if (opcount_ >= count_) {
        LOG(FATAL) << "No reply for op index: " << opcount_;
    }
    nfs_cb_opnum4 rop;
    xdr(reinterpret_cast<uint32_t&>(rop), xdrs_);
    if (rop != op) {
        LOG(FATAL) << "Unexpected op in CB_COMPOUND4res";
    }
    nfsstat4 status;
    xdr(status, xdrs_);
    opcount_++;

    return status;
}

void CallbackReplyDecoder::check(nfs_cb_opnum4 op)
{
    auto st = status(op);
    if (st != NFS4_OK)
        throw st;
}

template <typename ResokType>
ResokType CallbackReplyDecoder::get(nfs_cb_opnum4 op)
{
    check(op);
    ResokType resok;
    xdr(resok, xdrs_);
    return resok;
}

CB_GETATTR4resok CallbackReplyDecoder::getattr()
{
    return get<CB_GETATTR4resok>(OP_CB_GETATTR);
}

void CallbackReplyDecoder::recall()
{
    check(OP_CB_RECALL);
}

void CallbackReplyDecoder::layoutrecall()
{
    check(OP_CB_LAYOUTRECALL);
}

CB_SEQUENCE4resok CallbackReplyDecoder::sequence()
{
    return get<CB_SEQUENCE4resok>(OP_CB_SEQUENCE);
}

void CallbackReplyDecoder::notify_deviceid()
{
    check(OP_CB_NOTIFY_DEVICEID);
}

void CallbackReplyDecoder::recall_any()
{
    check(OP_CB_RECALL_ANY);
}
