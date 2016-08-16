#-
# Copyright (c) 2016 Doug Rabson
# All rights reserved.
#

cc_library(
    name = "nfs3",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp", "*.h"]) + [
        "//filesys:mount_prot",
        "//filesys:nfs_prot"
    ],
    deps = [
        "//filesys:hdrs",
        "//filesys/pfs",
        "//util",
        "//external:rpcxx",
        "//external:glog"
    ],
    visibility = ["//visibility:public"]
)

test_suite(
    name = "small",
    tags = ["small"]
)

cc_test(
    name = "nfs3_test",
    size = "small",
    copts = ["-std=c++14"],
    srcs = glob(["test/*.cpp"]),
    linkstatic = 1,
    deps = [
        ":nfs3",
        "//external:rpcxx",
        "//external:gmock_main"
    ],
    args = ["--gmock_verbose=error"]
)
