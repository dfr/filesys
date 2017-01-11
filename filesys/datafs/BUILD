#-
# Copyright (c) 2016-2017 Doug Rabson
# All rights reserved.
#

cc_library(
    name = "datafs",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp", "*.h"]),
    hdrs = ["datafs.h"],
    deps = [
        "//filesys:hdrs",
	"//filesys:access",
	"//filesys/posix",
        "//util",
        "//external:glog"
    ],
    visibility = ["//visibility:public"]
)
