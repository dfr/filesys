#-
# Copyright (c) 2016-2017 Doug Rabson
# All rights reserved.
#

cc_library(
    name = "objfs",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp"]) + [":proto"],
    hdrs = ["objfs.h", "objfskey.h"],
    deps = [
        ":hdrs",
        "//external:rpcxx",
        "//filesys:hdrs",
        "//filesys:access",
        "//keyval",
        "//util",
        "//external:glog",
    ],
    visibility = ["//visibility:public"]
)

cc_library(
    name = "hdrs",
    hdrs = glob(["*.h"]),
)

cc_binary(
    name = "objfsck",
    copts = ["-std=c++14"],
    srcs = glob(["check/*.cpp", "rocksdbi.*"]),
    deps = [
        ":objfs",
        "//keyval",
        "//external:rpcxx",
        "//external:glog",
    ],
    visibility = ["//visibility:public"],
)

genrule(
    name = "proto",
    srcs = ["objfsproto.x"],
    outs = ["objfsproto.h"],
    tools = ["//external:rpcgen"],
    cmd = "$(location //external:rpcgen) -tx -n filesys::objfs $(SRCS) > $(OUTS)",
    visibility = ["//visibility:public"]
)

test_suite(
    name = "small",
    tags = ["small"]
)

cc_test(
    name = "objfs_test",
    size = "small",
    copts = ["-std=c++14"],
    srcs = glob(["test/*.cpp"]),
    deps = [
        ":objfs",
        "//external:gtest"
    ],
    linkstatic = 1,
)
