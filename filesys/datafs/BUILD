cc_library(
    name = "datafs",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp", "*.h"]),
    hdrs = ["datafs.h"],
    deps = [
        "//filesys:hdrs",
	"//filesys:access",
	"//filesys/posix",
        "//third_party/glog:glog"
    ],
    visibility = ["//visibility:public"]
)
