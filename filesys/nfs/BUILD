cc_library(
    name = "nfs",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp", "*.h"]) + [
        "//filesys:mount_prot",
        "//filesys:nfs_prot"
    ],
    deps = [
        "//filesys:hdrs",
        "//filesys/pfs",
        "//rpcxx",
        "//third_party/glog:glog"
    ],
    visibility = ["//visibility:public"]
)

test_suite(
    name = "small",
    tags = ["small"]
)

cc_test(
    name = "nfs_test",
    size = "small",
    copts = ["-std=c++14"],
    srcs = glob(["test/*.cpp"]),
    linkstatic = 1,
    deps = [
        ":nfs",
        "//rpcxx",
        "//third_party/gmock:gmock_main"
    ],
    args = ["--gmock_verbose=error"]
)
