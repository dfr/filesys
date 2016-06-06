/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

// -*- c++ -*-
#pragma once

namespace filesys {
namespace nfs4 {

class CompoundRequestEncoder
{
public:
    CompoundRequestEncoder(const std::string& tag, oncrpc::XdrSink* xdrs)
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

    ~CompoundRequestEncoder()
    {
        *opcountp_ = opcount_;
    }

    void add(nfs_opnum4 op)
    {
        xdr(reinterpret_cast<uint32_t&>(op), xdrs_);
        opcount_++;
    }

    template <typename... Args>
        void add(nfs_opnum4 op, const Args&... args)
    {
        xdr(reinterpret_cast<uint32_t&>(op), xdrs_);
        encodeArgs(args...);
        opcount_++;
    }

    void encodeArgs()
    {
    }

    template <typename T, typename... Rest>
    void encodeArgs(const T& arg, const Rest&... rest)
    {
        xdr(arg, xdrs_);
        encodeArgs(rest...);
    }

    void access(uint32_t mode)
    {
        add(OP_ACCESS, mode);
    }

    void close(seqid4 seqid, stateid4 open_stateid)
    {
        add(OP_CLOSE, seqid, open_stateid);
    }

    void commit(offset4 offset, count4 count)
    {
        add(OP_COMMIT, offset, count);
    }

    void create(
        const createtype4& objtype, const component4& objname,
        const fattr4& createttrs)
    {
        add(OP_CREATE, objtype, objname, createttrs);
    }

    void delegpurge(clientid4 clientid)
    {
        add(OP_DELEGPURGE, clientid);
    }

    void delegreturn(stateid4 deleg_stateid)
    {
        add(OP_DELEGRETURN, deleg_stateid);
    }

    void getattr(const bitmap4& attr_request)
    {
        add(OP_GETATTR, attr_request);
    }

    void getfh()
    {
        add(OP_GETFH);
    }

    void link(const component4& newname)
    {
        add(OP_LINK, newname);
    }

    void lock(
        nfs_lock_type4 locktype, bool reclaim, offset4 offset, length4 length,
        locker4 locker)
    {
        add(OP_LOCK, locktype, reclaim, offset, length, locker);
    }

    void lockt(
        nfs_lock_type4 locktype, offset4 offset, length4 length,
        lock_owner4 owner)
    {
        add(OP_LOCKT, locktype, offset, length, owner);
    }

    void locku(
        nfs_lock_type4 locktype, seqid4 seqid, stateid4 lock_stateid,
        offset4 offset, length4 length)
    {
        add(OP_LOCKU, locktype, seqid, lock_stateid, offset, length);
    }

    void lookup(const component4& objname)
    {
        add(OP_LOOKUP, objname);
    }

    void lookupp()
    {
        add(OP_LOOKUPP);
    }

    void nverify(const fattr4& obj_attributes)
    {
        add(OP_NVERIFY, obj_attributes);
    }

    void open(
        seqid4 seqid, uint32_t share_access, uint32_t share_deny,
        open_owner4 owner, openflag4 openhow, open_claim4 claim)
    {
        add(OP_OPEN, seqid, share_access, share_deny, owner, openhow, claim);
    }

    void openattr(bool createdir)
    {
        add(OP_OPENATTR, createdir);
    }

    void open_confirm(stateid4 open_stateid, seqid4 seqid)
    {
        add(OP_OPEN_CONFIRM, open_stateid, seqid);
    }

    void open_downgrade(
        stateid4 open_stateid, seqid4 seqid, uint32_t share_access,
        uint32_t share_deny)
    {
        add(OP_OPEN_DOWNGRADE, open_stateid, seqid, share_access, share_deny);
    }

    void putfh(const nfs_fh4& object)
    {
        add(OP_PUTFH, object);
    }

    void putpubfh()
    {
        add(OP_PUTPUBFH);
    }

    void putrootfh()
    {
        add(OP_PUTROOTFH);
    }

    void read(stateid4 stateid, offset4 offset, count4 count)
    {
        add(OP_READ, stateid, offset, count);
    }

    void readdir(
        nfs_cookie4 cookie, const verifier4& cookieverf, count4 dircount,
        count4 maxcount, const bitmap4& attr_request)
    {
        add(OP_READDIR, cookie, cookieverf, dircount, maxcount, attr_request);
    }

    void readlink()
    {
        add(OP_READLINK);
    }

    void remove(const component4& target)
    {
        add(OP_REMOVE, target);
    }

    void rename(const component4& oldname, const component4& newname)
    {
        add(OP_RENAME, oldname, newname);
    }

    void renew(clientid4 clientid)
    {
        add(OP_RENEW, clientid);
    }

    void restorefh()
    {
        add(OP_RESTOREFH);
    }

    void savefh()
    {
        add(OP_SAVEFH);
    }

    void secinfo(const component4& name)
    {
        add(OP_SECINFO, name);
    }

    void setattr(const stateid4& stateid, const fattr4& attrset)
    {
        add(OP_SETATTR, stateid, attrset);
    }

    void setclientid(
        const nfs_client_id4& client, const cb_client4& callback,
        uint32_t callback_ident)
    {
        add(OP_SETCLIENTID, client, callback, callback_ident);
    }

    void setclientid_confirm(
        clientid4 clientid, verifier4 setclientid_confirm)
    {
        add(OP_SETCLIENTID_CONFIRM, clientid, setclientid_confirm);
    }

    void verify(const fattr4& obj_attributes)
    {
        add(OP_VERIFY, obj_attributes);
    }

    void write(
        stateid4 stateid, offset4 offset, stable_how4 stable,
        std::shared_ptr<oncrpc::Buffer> data)
    {
        add(OP_WRITE, stateid, offset, stable, data);
    }

    void release_lockowner(const lock_owner4& lock_owner)
    {
        add(OP_RELEASE_LOCKOWNER, lock_owner);
    }

    void backchannel_ctl(
        uint32_t cb_program,
        const std::vector<callback_sec_parms4>& sec_parms)
    {
        add(OP_BACKCHANNEL_CTL, cb_program, sec_parms);
    }

    void bind_conn_to_session(
        const sessionid4& sessid, channel_dir_from_client4 dir,
        bool use_conn_in_rdma_mode)
    {
        add(OP_BIND_CONN_TO_SESSION, sessid, dir, use_conn_in_rdma_mode);
    }

    void exchange_id(
        const client_owner4& clientowner, uint32_t flags,
        state_protect4_a state_protect,
        const std::vector<nfs_impl_id4> client_impl_id)
    {
        add(OP_EXCHANGE_ID, clientowner, flags, state_protect, client_impl_id);
    }

    void create_session(
        clientid4 clientid, sequenceid4 sequence, uint32_t flags,
        const channel_attrs4& fore_chan_attrs,
        const channel_attrs4& back_chan_attrs,
        uint32_t cb_program, const std::vector<callback_sec_parms4>& sec_parms)
    {
        add(OP_CREATE_SESSION, clientid, sequence, flags, fore_chan_attrs,
            back_chan_attrs, cb_program, sec_parms);
    }

    void destroy_session(sessionid4 sessionid)
    {
        add(OP_DESTROY_SESSION, sessionid);
    }

    void free_stateid(stateid4 stateid)
    {
        add(OP_FREE_STATEID, stateid);
    }

    // get_dir_delegation
    // getdeviceinfo
    // getdevicelist

    void layoutcommit(
        offset4 offset, length4 length, bool reclaim, const stateid4& stateid,
        const newoffset4& last_write_offset, const newtime4& time_modify,
        const layoutupdate4& layoutupdate)
    {
        add(OP_LAYOUTCOMMIT, offset, length, reclaim, stateid,
            last_write_offset, time_modify, layoutupdate);
    }

    void layoutget(
        bool signal_layout_avail, layouttype4 layout_type,
        layoutiomode4 iomode, offset4 offset, length4 length,
        length4 minlength, const stateid4& stateid, count4 maxcount)
    {
        add(OP_LAYOUTGET, signal_layout_avail, layout_type, iomode, offset,
            length, minlength, stateid, maxcount);
    }

    void layoutreturn(
        bool reclaim, layouttype4 layout_type, layoutiomode4 iomode,
        const layoutreturn4& layoutreturn)
    {
        add(OP_LAYOUTRETURN, reclaim, layout_type, iomode, layoutreturn);
    }

    void sequence(
        const sessionid4& sessionid, sequenceid4 sequenceid, slotid4 slotid,
        slotid4 highest_slotid, bool cachethis)
    {
        add(OP_SEQUENCE, sessionid, sequenceid, slotid, highest_slotid,
            cachethis);
    }

    void test_stateid(const std::vector<stateid4>& stateids)
    {
        add(OP_TEST_STATEID, stateids);
    }

    // want_delegation

    void destroy_clientid(clientid4 clientid)
    {
        add(OP_DESTROY_CLIENTID, clientid);
    }

    // reclaim_complete

private:
    const std::string& tag_;
    oncrpc::XdrSink* xdrs_;
    oncrpc::XdrWord* opcountp_;
    int opcount_;
};

class CompoundReplyDecoder
{
public:
    CompoundReplyDecoder(const std::string& tag, oncrpc::XdrSource* xdrs)
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

    auto status() const { return status_; }

    nfsstat4 status(nfs_opnum4 op)
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

    void check(nfs_opnum4 op)
    {
        auto st = status(op);
        if (st != NFS4_OK)
            throw st;
    }

    template <typename ResokType> auto get(nfs_opnum4 op)
    {
        check(op);
        ResokType resok;
        xdr(resok, xdrs_);
        return resok;
    }

    ACCESS4resok access()
    {
        return get<ACCESS4resok>(OP_ACCESS);
    }

    stateid4 close()
    {
        return get<stateid4>(OP_CLOSE);
    }

    COMMIT4resok commit()
    {
        return get<COMMIT4resok>(OP_COMMIT);
    }

    CREATE4resok create()
    {
        return get<CREATE4resok>(OP_CREATE);
    }

    void delegpurge()
    {
        check(OP_DELEGPURGE);
    }

    void delegreturn()
    {
        check(OP_DELEGRETURN);
    }

    GETATTR4resok getattr()
    {
        return get<GETATTR4resok>(OP_GETATTR);
    }

    GETFH4resok getfh()
    {
        return get<GETFH4resok>(OP_GETFH);
    }

    LINK4resok link()
    {
        return get<LINK4resok>(OP_LINK);
    }

    LOCK4resok lock()
    {
        return get<LOCK4resok>(OP_LOCK);
    }

    void lockt()
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

    stateid4 locku()
    {
        return get<stateid4>(OP_LOCKU);
    }

    void lookup()
    {
        check(OP_LOOKUP);
    }

    void lookupp()
    {
        check(OP_LOOKUPP);
    }

    nfsstat4 nverify()
    {
        return status(OP_NVERIFY);
    }

    OPEN4resok open()
    {
        return get<OPEN4resok>(OP_OPEN);
    }

    void openattr()
    {
        check(OP_OPENATTR);
    }

    OPEN_CONFIRM4resok open_confirm()
    {
        return get<OPEN_CONFIRM4resok>(OP_OPEN_CONFIRM);
    }

    OPEN_DOWNGRADE4resok open_downgrade()
    {
        return get<OPEN_DOWNGRADE4resok>(OP_OPEN_DOWNGRADE);
    }

    void putfh()
    {
        check(OP_PUTFH);
    }

    void putpubfh()
    {
        check(OP_PUTPUBFH);
    }

    void putrootfh()
    {
        check(OP_PUTROOTFH);
    }

    READ4resok read()
    {
        return get<READ4resok>(OP_READ);
    }

    READDIR4resok readdir()
    {
        return get<READDIR4resok>(OP_READDIR);
    }

    READLINK4resok readlink()
    {
        return get<READLINK4resok>(OP_READLINK);
    }

    REMOVE4resok remove()
    {
        return get<REMOVE4resok>(OP_REMOVE);
    }

    RENAME4resok rename()
    {
        return get<RENAME4resok>(OP_RENAME);
    }

    void renew()
    {
        check(OP_RENEW);
    }

    void restorefh()
    {
        check(OP_RESTOREFH);
    }

    void savefh()
    {
        check(OP_SAVEFH);
    }

    SECINFO4resok secinfo()
    {
        return get<SECINFO4resok>(OP_SECINFO);
    }

    bitmap4 setattr()
    {
        return get<bitmap4>(OP_SETATTR);
    }

    SETCLIENTID4resok setclientid()
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

    void setclientid_confirm()
    {
        check(OP_SETCLIENTID_CONFIRM);
    }

    nfsstat4 verify()
    {
        return status(OP_VERIFY);
    }

    WRITE4resok write()
    {
        return get<WRITE4resok>(OP_WRITE);
    }

    void release_lockowner()
    {
        check(OP_RELEASE_LOCKOWNER);
    }

    void backchannel_ctl()
    {
        check(OP_BACKCHANNEL_CTL);
    }

    BIND_CONN_TO_SESSION4resok bind_conn_to_session()
    {
        return get<BIND_CONN_TO_SESSION4resok>(OP_BIND_CONN_TO_SESSION);
    }

    EXCHANGE_ID4resok exchange_id()
    {
        return get<EXCHANGE_ID4resok>(OP_EXCHANGE_ID);
    }

    CREATE_SESSION4resok create_session()
    {
        return get<CREATE_SESSION4resok>(OP_CREATE_SESSION);
    }

    void destroy_session()
    {
        check(OP_DESTROY_SESSION);
    }

    void free_stateid()
    {
        check(OP_FREE_STATEID);
    }

    // get_dir_delegation
    // getdeviceinfo
    // getdevicelist

    LAYOUTCOMMIT4resok layoutcommit()
    {
        return get<LAYOUTCOMMIT4resok>(OP_LAYOUTCOMMIT);
    }

    LAYOUTGET4resok layoutget()
    {
        return get<LAYOUTGET4resok>(OP_LAYOUTGET);
    }

    layoutreturn_stateid layoutreturn()
    {
        return get<layoutreturn_stateid>(OP_LAYOUTRETURN);
    }

    SEQUENCE4resok sequence()
    {
        return get<SEQUENCE4resok>(OP_SEQUENCE);
    }

    TEST_STATEID4resok test_stateid()
    {
        return get<TEST_STATEID4resok>(OP_TEST_STATEID);
    }

    // want_delegation

    void destroy_clientid()
    {
        check(OP_DESTROY_CLIENTID);
    }

    // reclaim_complete

private:
    const std::string& tag_;
    oncrpc::XdrSource* xdrs_;
    nfsstat4 status_;
    int opcount_ = 0;
    int count_;
};

class CallbackRequestEncoder
{
public:
    CallbackRequestEncoder(const std::string& tag, oncrpc::XdrSink* xdrs)
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

    ~CallbackRequestEncoder()
    {
        *opcountp_ = opcount_;
    }

    void add(nfs_cb_opnum4 op)
    {
        xdr(reinterpret_cast<uint32_t&>(op), xdrs_);
        opcount_++;
    }

    template <typename... Args>
        void add(nfs_cb_opnum4 op, const Args&... args)
    {
        xdr(reinterpret_cast<uint32_t&>(op), xdrs_);
        encodeArgs(args...);
        opcount_++;
    }

    void encodeArgs()
    {
    }

    template <typename T, typename... Rest>
    void encodeArgs(const T& arg, const Rest&... rest)
    {
        xdr(arg, xdrs_);
        encodeArgs(rest...);
    }

    void getattr(const nfs_fh4& fh, const bitmap4& attr_request)
    {
        add(OP_CB_GETATTR, fh, attr_request);
    }

    void recall(const stateid4& stateid, bool truncate, const nfs_fh4& fh)
    {
        add(OP_CB_RECALL, stateid, truncate, fh);
    }

    void layoutrecall(
        layouttype4 type, layoutiomode4 iomode, bool changed,
        const layoutrecall4& recall)
    {
        add(OP_CB_LAYOUTRECALL, type, iomode, changed, recall);
    }

    void sequence(
        const sessionid4& session, sequenceid4 sequence,
        slotid4 slotid, slotid4 highest_slotid, bool cachethis,
        std::vector<referring_call_list4> referring_call_lists)
    {
        add(OP_CB_SEQUENCE, session, sequence, slotid, highest_slotid,
            cachethis, referring_call_lists);
    }

    void notify_deviceid(const std::vector<notify4>& changes)
    {
        add(OP_CB_NOTIFY_DEVICEID, changes);
    }

private:
    const std::string& tag_;
    oncrpc::XdrSink* xdrs_;
    oncrpc::XdrWord* opcountp_;
    int opcount_;
};

class CallbackReplyDecoder
{
public:
    CallbackReplyDecoder(const std::string& tag, oncrpc::XdrSource* xdrs)
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

    auto status() const { return status_; }

    nfsstat4 status(nfs_cb_opnum4 op)
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

    void check(nfs_cb_opnum4 op)
    {
        auto st = status(op);
        if (st != NFS4_OK)
            throw st;
    }

    template <typename ResokType> auto get(nfs_cb_opnum4 op)
    {
        check(op);
        ResokType resok;
        xdr(resok, xdrs_);
        return resok;
    }

    CB_GETATTR4resok getattr()
    {
        return get<CB_GETATTR4resok>(OP_CB_GETATTR);
    }

    void recall()
    {
        check(OP_CB_RECALL);
    }

    void layoutrecall()
    {
        check(OP_CB_LAYOUTRECALL);
    }

    CB_SEQUENCE4resok sequence()
    {
        return get<CB_SEQUENCE4resok>(OP_CB_SEQUENCE);
    }

    void notify_deviceid()
    {
        check(OP_CB_NOTIFY_DEVICEID);
    }

private:
    const std::string& tag_;
    oncrpc::XdrSource* xdrs_;
    nfsstat4 status_;
    int opcount_ = 0;
    int count_;
};

}
}
