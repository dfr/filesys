#-
# Copyright (c) 2016-2017 Doug Rabson
# All rights reserved.
#

cc_library(
    name = "keyval",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp"]),
    deps = [
        ":hdrs",
        "//keyval/mem",
        "//keyval/paxos",
        "//keyval/rocks",
        "//util",
    ],
    visibility = ["//visibility:public"]
)

cc_inc_library(
    name = "hdrs",
    hdrs = glob(["include/*.h"]),
    prefix = "include",
    deps = [
        "//external:rpcxx",
    ],
    visibility = ["//visibility:public"]
)
