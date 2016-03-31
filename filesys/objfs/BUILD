cc_library(
    name = "objfs",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp"]) + [":proto"],
    hdrs = ["objfs.h"],
    deps = [
        ":hdrs",
        "//rpcxx",
        "//filesys:hdrs",
        "//filesys:access",
        "//keyval",
        "//third_party/glog",
    ],
    visibility = ["//visibility:public"]
)

cc_library(
    name = "hdrs",
    hdrs = glob(["*.h"]),
)

cc_binary(
    name = "objfsck",
    copts = ["-std=c++14"],
    srcs = glob(["check/*.cpp", "rocksdbi.*"]),
    deps = [
        ":objfs",
        "//keyval",
        "//rpcxx",
        "//third_party/glog",
    ],
)

genrule(
    name = "proto",
    srcs = ["objfsproto.x"],
    outs = ["objfsproto.h"],
    tools = ["//rpcxx/utils/rpcgen"],
    cmd = "$(location //rpcxx/utils/rpcgen) -tx -n filesys::objfs $(SRCS) > $(OUTS)",
)

test_suite(
    name = "small",
    tags = ["small"]
)

cc_test(
    name = "objfs_test",
    size = "small",
    copts = ["-std=c++14"],
    srcs = glob(["test/*.cpp"]),
    deps = [
        ":objfs",
        "//third_party/gtest:gtest_main"
    ],
    linkstatic = 1,
)
