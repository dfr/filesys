#-
# Copyright (c) 2016 Doug Rabson
# All rights reserved.
#

cc_library(
    name = "rocks",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp"]),
    hdrs = ["rocks.h"],
    deps = [
        "//keyval:hdrs",
        "//external:glog",
        "//external:rocksdb",
    ],
    visibility = ["//visibility:public"]
)
