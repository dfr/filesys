/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <glog/logging.h>

#include "client.h"
#include "session.h"
#include "state.h"

using namespace filesys::nfs4;
using namespace nfsd::nfs4;
using namespace std;

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
    assert(type_ == DELEGATION || type_ == LAYOUT);
    shared_ptr<NfsSession> session;
    for (auto s: client_.lock()->sessions()) {
        if (s->hasBackChannel()) {
            session = s;
            break;
        }
    }
    if (!session) {
        LOG(ERROR) << "No back channel to send recall";
        return;
    }

    if (!recalled_) {
        if (type_ == DELEGATION) {
            VLOG(1) << "Recalling delegation: " << id_;
            session->recallDelegation(id_, fs_.lock()->fh());
        }
        else {
            updateLayout();
            VLOG(1) << "Recalling layout: " << id_;
            try {
                session->recallLayout(
                    LAYOUT4_FLEX_FILES,
                    ((access_ == OPEN4_SHARE_ACCESS_BOTH)
                     ? LAYOUTIOMODE4_RW : LAYOUTIOMODE4_READ),
                    false,
                    layoutrecall4(
                        LAYOUTRECALL4_FILE,
                        layoutrecall_file4{
                            fs_.lock()->fh(), offset_, length_, id_}));
            }
            catch (nfsstat4 stat) {
                if (stat == NFS4ERR_NOMATCHING_LAYOUT) {
                    // Client seems to have forgotten about the layout -
                    // revoke it now
                    LOG(ERROR) << "Client returned NFS4ERR_NOMATCHING_LAYOUT:"
                               << " revoking layout now";
                    auto cl = client_.lock();
                    if (cl) {
                        cl->revokeState(shared_from_this());
                        cl->clearState(id_);
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
    // If the new access mode is less restrictive than the
    // previous, we need to keep the new open owner
    if ((access_ | access) != access_) {
        of_ = of;
    }

    access_ |= access;
    deny_ |= deny;
    id_.seqid++;
}

void NfsState::updateDelegation(
    int access,  shared_ptr<filesys::OpenFile> of)
{
    if (access_ != access) {
        access_ = access;
        of_ = of;
        id_.seqid++;
    }
}
