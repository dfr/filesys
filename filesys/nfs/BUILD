#-
# Copyright (c) 2016-2017 Doug Rabson
# All rights reserved.
#

cc_library(
    name = "nfs",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp", "*.h"]),
    deps = [
        "//filesys:hdrs",
	"//filesys:access",
        "//filesys:xdr",
	"//filesys/nfs3",
	"//filesys/nfs4",
        "//external:glog"
    ],
    visibility = ["//visibility:public"]
)
