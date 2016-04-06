cc_library(
    name = "nfs3",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp", "*.h"]) + [
        "//filesys:mount_prot",
        "//filesys:nfs_prot"
    ],
    deps = [
        "//filesys:hdrs",
        "//filesys:urlparser",
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
    name = "nfs3_test",
    size = "small",
    copts = ["-std=c++14"],
    srcs = glob(["test/*.cpp"]),
    linkstatic = 1,
    deps = [
        ":nfs3",
        "//rpcxx",
        "//third_party/gmock:gmock_main"
    ],
    args = ["--gmock_verbose=error"]
)
