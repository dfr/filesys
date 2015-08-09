cc_library(
    name = "nfs",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp"]) + [":mount"] + [":nfs_prot"],
    deps = [
        "//include:hdrs",
        "//src/pfs:pfs",
        "//rpcxx:rpcxx",
        "//third_party/glog:glog"
    ],
    visibility = ["//visibility:public"]
)

genrule(
    name = "mount",
    srcs = ["mount.x"],
    outs = ["mount.h"],
    tools = ["//rpcxx/utils/rpcgen"],
    cmd = "$(location //rpcxx/utils/rpcgen) -txic -n filesys::nfs $(location mount.x) > $(OUTS)",
)

genrule(
    name = "nfs_prot",
    srcs = ["nfs_prot.x"],
    outs = ["nfs_prot.h"],
    tools = ["//rpcxx/utils/rpcgen"],
    cmd = "$(location //rpcxx/utils/rpcgen) -txic -n filesys::nfs $(location nfs_prot.x) > $(OUTS)",
)
