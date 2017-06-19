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

#include <keyval/keyval.h>
#include <glog/logging.h>

#include "client.h"
#include "session.h"
#include "state.h"

using namespace filesys::nfs4;
using namespace nfsd::nfs4;
using namespace std;

NfsState::NfsState(
    shared_ptr<keyval::Namespace> stateNS,
    StateType type,
    const filesys::nfs4::stateid4& id,
    std::shared_ptr<NfsClient> client,
    std::shared_ptr<NfsFileState> fs,
    const filesys::nfs4::state_owner4& owner,
    int access,
    int deny,
    std::shared_ptr<filesys::OpenFile> of,
    util::Clock::time_point expiry)
    : stateNS_(stateNS),
      type_(type),
      id_(id),
      client_(client),
      fs_(fs),
      owner_(owner),
      data_({access, deny}),
      of_(of),
      expiry_(expiry)
{
}

void NfsState::save(keyval::Transaction* trans)
{
    if (revoked_)
        return;

    if (trans) {
        StateKey sk;
        sk.fh = fs()->fh();
        sk.type = type_;
        sk.clientOwner = client()->owner().co_ownerid;
        if (type_ == StateType::OPEN)
            sk.stateOwner = owner_.owner;

        auto key = make_shared<oncrpc::Buffer>(oncrpc::XdrSizeof(sk));
        auto value = make_shared<oncrpc::Buffer>(oncrpc::XdrSizeof(data_));
        oncrpc::XdrMemory xmk(key->data(), key->size());
        oncrpc::XdrMemory xmd(value->data(), value->size());
        xdr(sk, static_cast<oncrpc::XdrSink*>(&xmk));
        xdr(data_, static_cast<oncrpc::XdrSink*>(&xmd));
        trans->put(stateNS_, key, value);
    }
}

void NfsState::remove(keyval::Transaction* trans)
{
    if (revoked_)
        return;

    if (trans) {
        StateKey sk;
        auto p = fs();
        if (p) {
            sk.fh = p->fh();
            sk.type = type_;
            sk.clientOwner = client()->owner().co_ownerid;
            if (type_ == StateType::OPEN)
                sk.stateOwner = owner_.owner;

            auto key = make_shared<oncrpc::Buffer>(oncrpc::XdrSizeof(sk));
            oncrpc::XdrMemory xmk(key->data(), key->size());
            xdr(sk, static_cast<oncrpc::XdrSink*>(&xmk));
            trans->remove(stateNS_, key);
        }
    }
}

void NfsState::revoke()
{
    if (!revoked_) {
        fs_.reset();
        of_.reset();
        revoked_ = true;
    }
}

void NfsState::recall()
{
    assert(type_ == StateType::DELEGATION || type_ == StateType::LAYOUT);

    if (recalled_ || revoked_)
        return;

    // If this is a restored state entry which has not yet been
    // reclaimed, just mark as recalled since the client hasn't yet
    // re-connected
    if (restored_) {
        VLOG(1) << "Revoking un-reclaimed state: " << id_;
        auto cl = client_.lock();
        if (cl) {
            cl->revokeState(shared_from_this());
            cl->clearState(id_);
        }
        return;
    }

    shared_ptr<NfsSession> session;
    for (auto s: client_.lock()->sessions()) {
        if (s->hasBackChannel()) {
            session = s;
            break;
        }
    }
    if (!session) {
        VLOG(1) << "No back channel to send recall, revoking: " << id_;
        auto cl = client_.lock();
        if (cl) {
            cl->revokeState(shared_from_this());
            cl->clearState(id_);
        }
        return;
    }

    if (!recalled_) {
        if (type_ == StateType::DELEGATION) {
            VLOG(1) << "Recalling delegation: " << id_;
            session->recallDelegation(id_, fs_.lock()->fh());
        }
        else {
            updateLayout();
            VLOG(1) << "Recalling layout: " << id_;
            try {
                session->recallLayout(
                    LAYOUT4_FLEX_FILES,
                    ((data_.access == OPEN4_SHARE_ACCESS_BOTH)
                     ? LAYOUTIOMODE4_RW : LAYOUTIOMODE4_READ),
                    false,
                    layoutrecall4(
                        LAYOUTRECALL4_FILE,
                        layoutrecall_file4{
                            fs_.lock()->fh(),
                            data_.offset, data_.length, id_}));
            }
            catch (nfsstat4 stat) {
                if (stat == NFS4ERR_NOMATCHING_LAYOUT) {
                    // Client seems to have forgotten about the layout -
                    // revoke it now
                    VLOG(1) << "Client returned NFS4ERR_NOMATCHING_LAYOUT:"
                            << " revoking layout now";
                    auto cl = client_.lock();
                    if (cl) {
                        cl->revokeAndClear(shared_from_this());
                    }
                }
            }
        }
        recalled_ = true;
    }
}

void NfsState::updateOpen(
    int access, int deny, shared_ptr<filesys::OpenFile> of)
{
    // If there is no change, ignore it and don't increment seqid.
    // Linux clients seem to get confused and use the older seqid in
    // this situation
    if (data_.access == access && data_.deny == deny)
        return;

    // If the new access mode is less restrictive than the
    // previous, we need to keep the new open owner
    if ((data_.access | access) != data_.access) {
        of_ = of;
    }

    data_.access |= access;
    data_.deny |= deny;
    id_.seqid++;
}

void NfsState::updateDelegation(
    int access,  shared_ptr<filesys::OpenFile> of)
{
    if (data_.access != access) {
        data_.access = access;
        of_ = of;
        id_.seqid++;
    }
}

void NfsState::setExpiry(util::Clock::time_point expiry)
{
    //LOG(INFO) << "Updating expiry for state: " << id_;
    auto client = client_.lock();
    if (client) {
        client->updateExpiry(shared_from_this(), expiry_, expiry);
    }
    expiry_ = expiry;
}
