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

#pragma once

namespace oncrpc {

class XdrSink;
class XdrSource;
class XdrWord;

}

namespace filesys {
namespace nfs4 {

class CompoundRequestEncoder
{
public:
    CompoundRequestEncoder(const std::string& tag, oncrpc::XdrSink* xdrs);

    ~CompoundRequestEncoder();

    void add(nfs_opnum4 op);

    template <typename... Args>
    void add(nfs_opnum4 op, const Args&... args);
    void encodeArgs() {}
    template <typename T, typename... Rest>
    void encodeArgs(const T& arg, const Rest&... rest);

    void access(uint32_t mode);
    void close(seqid4 seqid, stateid4 open_stateid);
    void commit(offset4 offset, count4 count);
    void create(
        const createtype4& objtype, const component4& objname,
        const fattr4& createttrs);
    void delegpurge(clientid4 clientid);
    void delegreturn(stateid4 deleg_stateid);
    void getattr(const bitmap4& attr_request);
    void getfh();
    void link(const component4& newname);
    void lock(
        nfs_lock_type4 locktype, bool reclaim, offset4 offset, length4 length,
        locker4 locker);
    void lockt(
        nfs_lock_type4 locktype, offset4 offset, length4 length,
        lock_owner4 owner);
    void locku(
        nfs_lock_type4 locktype, seqid4 seqid, stateid4 lock_stateid,
        offset4 offset, length4 length);
    void lookup(const component4& objname);
    void lookupp();
    void nverify(const fattr4& obj_attributes);
    void open(
        seqid4 seqid, uint32_t share_access, uint32_t share_deny,
        open_owner4 owner, openflag4 openhow, open_claim4 claim);
    void openattr(bool createdir);
    void open_confirm(stateid4 open_stateid, seqid4 seqid);
    void open_downgrade(
        stateid4 open_stateid, seqid4 seqid, uint32_t share_access,
        uint32_t share_deny);
    void putfh(const nfs_fh4& object);
    void putpubfh();
    void putrootfh();
    void read(stateid4 stateid, offset4 offset, count4 count);
    void readdir(
        nfs_cookie4 cookie, const verifier4& cookieverf, count4 dircount,
        count4 maxcount, const bitmap4& attr_request);
    void readlink();
    void remove(const component4& target);
    void rename(const component4& oldname, const component4& newname);
    void renew(clientid4 clientid);
    void restorefh();
    void savefh();
    void secinfo(const component4& name);
    void setattr(const stateid4& stateid, const fattr4& attrset);
    void setclientid(
        const nfs_client_id4& client, const cb_client4& callback,
        uint32_t callback_ident);
    void setclientid_confirm(
        clientid4 clientid, verifier4 setclientid_confirm);
    void verify(const fattr4& obj_attributes);
    void write(
        stateid4 stateid, offset4 offset, stable_how4 stable,
        std::shared_ptr<oncrpc::Buffer> data);
    void release_lockowner(const lock_owner4& lock_owner);
    void backchannel_ctl(
        uint32_t cb_program,
        const std::vector<callback_sec_parms4>& sec_parms);
    void bind_conn_to_session(
        const sessionid4& sessid, channel_dir_from_client4 dir,
        bool use_conn_in_rdma_mode);
    void exchange_id(
        const client_owner4& clientowner, uint32_t flags,
        state_protect4_a state_protect,
        const std::vector<nfs_impl_id4> client_impl_id);
    void create_session(
        clientid4 clientid, sequenceid4 sequence, uint32_t flags,
        const channel_attrs4& fore_chan_attrs,
        const channel_attrs4& back_chan_attrs,
        uint32_t cb_program, const std::vector<callback_sec_parms4>& sec_parms);
    void destroy_session(sessionid4 sessionid);
    void free_stateid(stateid4 stateid);
    // get_dir_delegation
    // getdeviceinfo
    // getdevicelist
    void layoutcommit(
        offset4 offset, length4 length, bool reclaim, const stateid4& stateid,
        const newoffset4& last_write_offset, const newtime4& time_modify,
        const layoutupdate4& layoutupdate);
    void layoutget(
        bool signal_layout_avail, layouttype4 layout_type,
        layoutiomode4 iomode, offset4 offset, length4 length,
        length4 minlength, const stateid4& stateid, count4 maxcount);
    void layoutreturn(
        bool reclaim, layouttype4 layout_type, layoutiomode4 iomode,
        const layoutreturn4& layoutreturn);
    void sequence(
        const sessionid4& sessionid, sequenceid4 sequenceid, slotid4 slotid,
        slotid4 highest_slotid, bool cachethis);
    void test_stateid(const std::vector<stateid4>& stateids);
    // want_delegation
    void destroy_clientid(clientid4 clientid);
    void reclaim_complete(bool one_fs);

private:
    const std::string& tag_;
    oncrpc::XdrSink* xdrs_;
    oncrpc::XdrWord* opcountp_;
    int opcount_;
};

class CompoundReplyDecoder
{
public:
    CompoundReplyDecoder(const std::string& tag, oncrpc::XdrSource* xdrs);

    auto status() const { return status_; }
    nfsstat4 status(nfs_opnum4 op);
    void check(nfs_opnum4 op);
    template <typename ResokType> ResokType get(nfs_opnum4 op);

    ACCESS4resok access();
    stateid4 close();
    COMMIT4resok commit();
    CREATE4resok create();
    void delegpurge();;
    void delegreturn();
    GETATTR4resok getattr();
    GETFH4resok getfh();
    LINK4resok link();
    LOCK4resok lock();
    void lockt();
    stateid4 locku();
    void lookup();
    void lookupp();
    nfsstat4 nverify();
    OPEN4resok open();
    void openattr();
    OPEN_CONFIRM4resok open_confirm();
    OPEN_DOWNGRADE4resok open_downgrade();
    void putfh();
    void putpubfh();
    void putrootfh();
    READ4resok read();
    READDIR4resok readdir();
    READLINK4resok readlink();
    REMOVE4resok remove();
    RENAME4resok rename();
    void renew();
    void restorefh();
    void savefh();
    SECINFO4resok secinfo();
    bitmap4 setattr();
    SETCLIENTID4resok setclientid();
    void setclientid_confirm();
    nfsstat4 verify();
    WRITE4resok write();
    void release_lockowner();
    void backchannel_ctl();
    BIND_CONN_TO_SESSION4resok bind_conn_to_session();
    EXCHANGE_ID4resok exchange_id();
    CREATE_SESSION4resok create_session();
    void destroy_session();
    void free_stateid();
    // get_dir_delegation
    // getdeviceinfo
    // getdevicelist
    LAYOUTCOMMIT4resok layoutcommit();
    LAYOUTGET4resok layoutget();
    layoutreturn_stateid layoutreturn();
    SEQUENCE4resok sequence();
    TEST_STATEID4resok test_stateid();
    // want_delegation
    void destroy_clientid();
    void reclaim_complete();

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
    CallbackRequestEncoder(const std::string& tag, oncrpc::XdrSink* xdrs);
    ~CallbackRequestEncoder();

    void add(nfs_cb_opnum4 op);
    template <typename... Args>
    void add(nfs_cb_opnum4 op, const Args&... args);
    void encodeArgs() {}
    template <typename T, typename... Rest>
    void encodeArgs(const T& arg, const Rest&... rest);

    void getattr(const nfs_fh4& fh, const bitmap4& attr_request);
    void recall(const stateid4& stateid, bool truncate, const nfs_fh4& fh);
    void layoutrecall(
        layouttype4 type, layoutiomode4 iomode, bool changed,
        const layoutrecall4& recall);
    void sequence(
        const sessionid4& session, sequenceid4 sequence,
        slotid4 slotid, slotid4 highest_slotid, bool cachethis,
        std::vector<referring_call_list4> referring_call_lists);
    void notify_deviceid(const std::vector<notify4>& changes);
    void recall_any(uint32_t objects_to_keep, const bitmap4& type_mask);

private:
    const std::string& tag_;
    oncrpc::XdrSink* xdrs_;
    oncrpc::XdrWord* opcountp_;
    int opcount_;
};

class CallbackReplyDecoder
{
public:
    CallbackReplyDecoder(const std::string& tag, oncrpc::XdrSource* xdrs);

    auto status() const { return status_; }
    nfsstat4 status(nfs_cb_opnum4 op);
    void check(nfs_cb_opnum4 op);
    template <typename ResokType> ResokType get(nfs_cb_opnum4 op);

    CB_GETATTR4resok getattr();
    void recall();
    void layoutrecall();
    CB_SEQUENCE4resok sequence();
    void notify_deviceid();
    void recall_any();

private:
    const std::string& tag_;
    oncrpc::XdrSource* xdrs_;
    nfsstat4 status_;
    int opcount_ = 0;
    int count_;
};

}
}
