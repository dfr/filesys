#-
# Copyright (c) 2016 Doug Rabson
# All rights reserved.
#

cc_library(
    name = "nfs4",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp", "*.h"]) + [
        "//filesys:nfs4_prot",
        "//filesys:flex_files_layout",
        "//filesys/distfs:proto",
    ],
    hdrs = ["nfs4fs.h", "nfs4proto.h", "nfs4util.h"],
    deps = [
        "//filesys:hdrs",
	"//filesys:access",
        "//filesys:xdr",
        "//util",
        "//external:glog"
    ],
    visibility = ["//visibility:public"]
)
