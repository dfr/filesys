#-
# Copyright (c) 2016 Doug Rabson
# All rights reserved.
#

cc_library(
    name = "nfs",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp", "*.h"]),
    deps = [
        "//filesys:hdrs",
	"//filesys:access",
        "//filesys:urlparser",
        "//filesys:xdr",
	"//filesys/nfs3",
	"//filesys/nfs4",
        "//third_party/glog:glog"
    ],
    visibility = ["//visibility:public"]
)
