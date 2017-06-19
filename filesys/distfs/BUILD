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
    name = "distfs",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp"]) + [":proto", "//filesys/objfs:proto"],
    hdrs = ["distfs.h"],
    deps = [
        ":hdrs",
        "//external:rpcxx",
        "//filesys:hdrs",
        "//filesys/nfs4",
        "//filesys/objfs",
        "//keyval",
        "//external:glog",
    ],
    visibility = ["//visibility:public"]
)

cc_library(
    name = "hdrs",
    hdrs = glob(["*.h"]),
)

genrule(
    name = "proto",
    srcs = ["distfsproto.x"],
    outs = ["distfsproto.h"],
    tools = ["//external:rpcgen"],
    cmd = "$(location //external:rpcgen) -txics -n filesys::distfs $(SRCS) > $(OUTS)",
    visibility = ["//visibility:public"]
)

test_suite(
    name = "small",
    tags = ["small"]
)

cc_test(
    name = "distfs_test",
    size = "small",
    copts = ["-std=c++14"],
    srcs = glob(["test/*.cpp"]),
    deps = [
        ":distfs",
	"//filesys",
	"//filesys/datafs",
        "//external:gtest"
    ],
    linkstatic = 1,
)

cc_binary(
    name = "distfsck",
    copts = ["-std=c++14"],
    srcs = glob(["check/*.cpp", "rocksdbi.*"]),
    deps = [
        ":distfs",
	"//filesys/nfs4",
        "//keyval",
        "//external:rpcxx",
        "//external:glog",
    ],
    visibility = ["//visibility:public"],
)
