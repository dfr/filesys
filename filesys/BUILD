cc_library(
    name = "filesys",
    copts = ["-std=c++14"],
    srcs = ["fsman.cpp"],
    deps = [
        ":hdrs",
        ":access",
        "//filesys/nfs",
        "//filesys/posix",
        "//filesys/objfs",
    ],
    visibility = ["//visibility:public"]
)

cc_library(
    name = "access",
    copts = ["-std=c++14"],
    srcs = ["checkaccess.cpp"],
    deps = [
        ":hdrs",
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

test_suite(
    name = "small",
    tags = ["small"]
)

cc_test(
    name = "filesys_test",
    size = "small",
    copts = ["-std=c++14"],
    srcs = glob(["test/*.cpp"]),
    deps = [
        ":filesys",
        "//third_party/gmock:gmock_main",
    ],
    linkstatic = 1,
)

genrule(
    name = "mount_prot",
    srcs = ["include/fs++/proto/mount.x"],
    outs = ["include/fs++/proto/mount.h"],
    tools = ["//rpcxx/utils/rpcgen"],
    cmd = "$(location //rpcxx/utils/rpcgen) -txics -n filesys::nfs $(location include/fs++/proto/mount.x) > $(OUTS)",
    visibility = ["//visibility:public"]
)

genrule(
    name = "nfs_prot",
    srcs = ["include/fs++/proto/nfs_prot.x"],
    outs = ["include/fs++/proto/nfs_prot.h"],
    tools = ["//rpcxx/utils/rpcgen"],
    cmd = "$(location //rpcxx/utils/rpcgen) -txics -n filesys::nfs $(location include/fs++/proto/nfs_prot.x) > $(OUTS)",
    visibility = ["//visibility:public"]
)
