#-
# Copyright (c) 2016-present Doug Rabson
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.
#

cc_library(
    name = "filesys",
    copts = ["-std=c++14"],
    srcs = ["fsman.cpp", "datastore.cpp"],
    deps = [
        ":hdrs",
        ":access",
        ":xdr",
        "//filesys/nfs",
        "//filesys/nfs3",
        "//filesys/nfs4",
        "//filesys/posix",
        "//filesys/objfs",
        "//filesys/datafs",
        "//filesys/distfs",
    ],
    visibility = ["//visibility:public"]
)

cc_library(
    name = "access",
    copts = ["-std=c++14"],
    srcs = ["checkaccess.cpp"],
    deps = [
        ":hdrs",
    ],
    visibility = ["//visibility:public"]
)

cc_library(
    name = "xdr",
    copts = ["-std=c++14"],
    srcs = ["xdr.cpp"],
    deps = [
        ":hdrs",
    ],
    visibility = ["//visibility:public"]
)

cc_inc_library(
    name = "hdrs",
    hdrs = glob(["include/*.h"]),
    prefix = "include",
    deps = ["//external:rpcxx"],
    visibility = ["//visibility:public"]
)

test_suite(
    name = "small",
    tags = ["small"]
)

cc_test(
    name = "filesys_test",
    size = "small",
    copts = ["-std=c++14"],
    srcs = glob(["test/*.cpp"]),
    deps = [
        ":filesys",
        "//external:gmock_main",
    ],
    linkstatic = 1,
)

genrule(
    name = "mount_prot",
    srcs = ["include/proto/mount.x"],
    outs = ["proto/mount.h"],
    tools = ["//external:rpcgen"],
    cmd = "$(location //external:rpcgen) -txics -n filesys::nfs3 $(location include/proto/mount.x) > $(OUTS)",
    visibility = ["//visibility:public"]
)

genrule(
    name = "nfs_prot",
    srcs = ["include/proto/nfs_prot.x"],
    outs = ["proto/nfs_prot.h"],
    tools = ["//external:rpcgen"],
    cmd = "$(location //external:rpcgen) -txics -n filesys::nfs3 $(location include/proto/nfs_prot.x) > $(OUTS)",
    visibility = ["//visibility:public"]
)

genrule(
    name = "nfs4_prot",
    srcs = ["include/proto/nfs4_prot.x"],
    outs = ["proto/nfs4_prot.h"],
    tools = ["//external:rpcgen"],
    cmd = "$(location //external:rpcgen) -txics -n filesys::nfs4 $(location include/proto/nfs4_prot.x) > $(OUTS)",
    visibility = ["//visibility:public"]
)

genrule(
    name = "flex_files_layout",
    srcs = ["include/proto/flex_files_layout.x"],
    outs = ["proto/flex_files_layout.h"],
    tools = ["//external:rpcgen"],
    cmd = "$(location //external:rpcgen) -txics -n filesys::nfs4 $(location include/proto/flex_files_layout.x) > $(OUTS)",
    visibility = ["//visibility:public"]
)
