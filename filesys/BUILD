cc_library(
    name = "filesys",
    copts = ["-std=c++14"],
    srcs = ["fsman.cpp"],
    deps = [
        ":hdrs",
        "//filesys/nfs",
        "//filesys/posix",
        "//filesys/objfs",
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
