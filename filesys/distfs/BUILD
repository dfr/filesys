#-
# Copyright (c) 2016-2017 Doug Rabson
# All rights reserved.
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
