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

#include <rpc++/rest.h>
#include "filesys/proto/nfs_prot.h"

namespace nfsd {
namespace nfs3 {

class NfsServer: public filesys::nfs3::NfsProgram3Service,
                 public oncrpc::RestHandler,
                 public std::enable_shared_from_this<NfsServer>
{
public:
    NfsServer(
        const std::vector<int>& sec,
        std::shared_ptr<filesys::Filesystem> fs);

    // INfsProgram3 overrides
    void dispatch(oncrpc::CallContext&& ctx) override;
    void null() override;
    filesys::nfs3::GETATTR3res getattr(const filesys::nfs3::GETATTR3args& args) override;
    filesys::nfs3::SETATTR3res setattr(const filesys::nfs3::SETATTR3args& args) override;
    filesys::nfs3::LOOKUP3res lookup(const filesys::nfs3::LOOKUP3args& args) override;
    filesys::nfs3::ACCESS3res access(const filesys::nfs3::ACCESS3args& args) override;
    filesys::nfs3::READLINK3res readlink(const filesys::nfs3::READLINK3args& args) override;
    filesys::nfs3::READ3res read(const filesys::nfs3::READ3args& args) override;
    filesys::nfs3::WRITE3res write(const filesys::nfs3::WRITE3args& args) override;
    filesys::nfs3::CREATE3res create(const filesys::nfs3::CREATE3args& args) override;
    filesys::nfs3::MKDIR3res mkdir(const filesys::nfs3::MKDIR3args& args) override;
    filesys::nfs3::SYMLINK3res symlink(const filesys::nfs3::SYMLINK3args& args) override;
    filesys::nfs3::MKNOD3res mknod(const filesys::nfs3::MKNOD3args& args) override;
    filesys::nfs3::REMOVE3res remove(const filesys::nfs3::REMOVE3args& args) override;
    filesys::nfs3::RMDIR3res rmdir(const filesys::nfs3::RMDIR3args& args) override;
    filesys::nfs3::RENAME3res rename(const filesys::nfs3::RENAME3args& args) override;
    filesys::nfs3::LINK3res link(const filesys::nfs3::LINK3args& args) override;
    filesys::nfs3::READDIR3res readdir(const filesys::nfs3::READDIR3args& args) override;
    filesys::nfs3::READDIRPLUS3res readdirplus(const filesys::nfs3::READDIRPLUS3args& args) override;
    filesys::nfs3::FSSTAT3res fsstat(const filesys::nfs3::FSSTAT3args& args) override;
    filesys::nfs3::FSINFO3res fsinfo(const filesys::nfs3::FSINFO3args& args) override;
    filesys::nfs3::PATHCONF3res pathconf(const filesys::nfs3::PATHCONF3args& args) override;
    filesys::nfs3::COMMIT3res commit(const filesys::nfs3::COMMIT3args& args) override;

    // RestHandler overrides
    bool get(
        std::shared_ptr<oncrpc::RestRequest> req,
        std::unique_ptr<oncrpc::RestEncoder>&& res) override;

    /// Set a REST registry object to allow access to the server state
    /// for monitoring and managment interfaces. This should be called
    /// on startup, before any clients are connected.
    void setRestRegistry(std::shared_ptr<oncrpc::RestRegistry> restreg)
    {
        assert(!restreg_.lock());
        restreg_ = restreg;
        restreg->add("/nfs3", true, shared_from_this());
    }

private:
    std::vector<int> sec_;
    std::weak_ptr<oncrpc::RestRegistry> restreg_;

    // Statistics
    std::vector<int> stats_;     // per-rpc op counts
};

}
}
