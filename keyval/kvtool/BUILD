#-
# Copyright (c) 2016-2017 Doug Rabson
# All rights reserved.
#

cc_binary(
    name = "kvtool",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp", "*.h", "commands/*.cpp"]),
    deps = [
        "//keyval",
        "//external:glog",
        "//external:gflags"
    ],
    linkstatic = 1,
    visibility = ["//visibility:public"],
)

