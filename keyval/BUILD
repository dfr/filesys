#-
# Copyright (c) 2016 Doug Rabson
# All rights reserved.
#

cc_library(
    name = "keyval",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp"]),
    deps = [
        ":hdrs",
        "//keyval/mem",
        "//keyval/rocks",
    ],
    visibility = ["//visibility:public"]
)

cc_library(
    name = "hdrs",
    hdrs = glob(["include/kv++/*.h"]),
    includes = ["include"],
    deps = [
        "//rpcxx",
    ],
    visibility = ["//visibility:public"]
)
