cc_library(
    name = "pfs",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp"]),
    hdrs = ["pfsfs.h"],
    deps = [
        "//include:hdrs",
        "//third_party/glog:glog"
    ],
    visibility = ["//visibility:public"]
)

cc_test(
    name = "pfs_test",
    size = "small",
    copts = ["-std=c++14"],
    srcs = glob(["test/*.cpp"]),
    deps = [
        ":pfs",
        "//third_party/gtest:gtest_main"
    ]
)
