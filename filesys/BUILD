cc_library(
    name = "filesys",
    copts = ["-std=c++14"],
    srcs = ["fsman.cpp"],
    deps = [
        ":hdrs",
        "//filesys/nfs",
        "//filesys/posix"
    ],
    visibility = ["//visibility:public"]
)

cc_library(
    name = "hdrs",
    hdrs = glob(["include/fs++/*.h"]),
    includes = ["include"],
    deps = ["//rpcxx"],
    visibility = ["//visibility:public"]
)
