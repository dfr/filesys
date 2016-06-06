#-
# Copyright (c) 2016 Doug Rabson
# All rights reserved.
#

cc_library(
    name = "posix",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp", "*.h"]),
    deps = [
        "//filesys:hdrs",
        "//third_party/glog:glog"
    ],
    visibility = ["//visibility:public"]
)
