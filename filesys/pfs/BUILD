cc_library(
    name = "pfs",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp", "*.h"]),
    hdrs = ["pfsfs.h"],
    deps = [
        "//filesys:hdrs",
	"//filesys:access",
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
    name = "pfs_test",
    size = "small",
    copts = ["-std=c++14"],
    srcs = glob(["test/*.cpp"]),
    deps = [
        ":pfs",
        "//third_party/gtest:gtest_main"
    ],
    linkopts = select ({
        "//:freebsd": ["-pthread"],
        "//:darwin": [],
    }),
    linkstatic = 1,
)
