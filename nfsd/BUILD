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

cc_binary(
    name = "nfsd",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp", "*.h"]) + [
        "//nfsd/ui",
    ],
    deps = [
        ":nfs3",
	":nfs4",
        ":threadpool",
        ":version",
        "//rpcxx",
        "//filesys",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "threadpool",
    copts = ["-std=c++14"],
    srcs = glob(["*.h"]),
)

cc_library(
    name = "version",
    copts = ["-std=c++14"],
    srcs = ["version.h"] + [":gen_version"],
    visibility = ["//visibility:public"]
)

cc_library(
    name = "nfs3",
    copts = ["-std=c++14"],
    srcs = glob([
        "nfs3/*.cpp",
        "nfs3/*.h",
    ]) + [
        "//filesys:mount_prot",
        "//filesys:nfs_prot"
    ],
    deps = [
        ":threadpool",
        "//rpcxx",
        "//filesys",
    ]
)

cc_library(
    name = "nfs4",
    copts = ["-std=c++14"],
    srcs = glob([
        "nfs4/*.cpp",
        "nfs4/*.h",
    ]) + [
        "//filesys:nfs4_prot"
    ],
    deps = [
        ":threadpool",
        ":version",
        "//rpcxx",
        "//filesys",
    ]
)

test_suite(
    name = "small",
    tags = ["small"]
)

cc_test(
    name = "nfsd_test",
    copts = ["-std=c++14"],
    size = "small",
    srcs = glob(["test/*.cpp"]),
    deps = [
        ":nfs3",
        ":nfs4",
        "//external:gtest_main",
    ],
    linkstatic = 1,
)

genrule(
    name = "gen_version",
    outs = ["version.cpp"],
    cmd = "$(location gen-version.sh) > $(OUTS)",
    tools = ["gen-version.sh"],
)
