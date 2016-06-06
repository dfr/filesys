#-
# Copyright (c) 2016 Doug Rabson
# All rights reserved.
#

cc_library(
    name = "distfs",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp"]) + [":proto", "//filesys/objfs:proto"],
    hdrs = ["distfs.h"],
    deps = [
        ":hdrs",
        "//rpcxx",
        "//filesys:hdrs",
        "//filesys/nfs4",
        "//filesys/objfs",
        "//keyval",
        "//third_party/glog",
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
    tools = ["//rpcxx/utils/rpcgen"],
    cmd = "$(location //rpcxx/utils/rpcgen) -txics -n filesys::distfs $(SRCS) > $(OUTS)",
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
        "//third_party/gtest"
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
        "//rpcxx",
        "//third_party/glog",
    ],
    visibility = ["//visibility:public"],
)
