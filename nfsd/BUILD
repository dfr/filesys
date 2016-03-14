cc_binary(
    name = "nfsd",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp", "*.h"]),
    deps = [
        ":nfs3",
	":nfs4",
        ":threadpool",
        "//rpcxx",
        "//filesys",
    ]
)

cc_library(
    name = "threadpool",
    copts = ["-std=c++14"],
    srcs = glob(["*.h"]),
)

cc_library(
    name = "nfs3",
    copts = ["-std=c++14"],
    srcs = glob([
        "nfs3/*.cpp",
        "nfs3/*.h",
    ]) + [
        "//filesys:mount_prot",
        "//filesys:nfs_prot"
    ],
    deps = [
        ":threadpool",
        "//rpcxx",
        "//filesys",
    ]
)

cc_library(
    name = "nfs4",
    copts = ["-std=c++14"],
    srcs = glob([
        "nfs4/*.cpp",
        "nfs4/*.h",
    ]) + [
        "//filesys:nfs4_prot"
    ],
    deps = [
        ":threadpool",
        "//rpcxx",
        "//filesys",
    ]
)

test_suite(
    name = "small",
    tags = ["small"]
)

cc_test(
    name = "nfsd_test",
    copts = ["-std=c++14"],
    size = "small",
    srcs = glob(["test/*.cpp"]),
    deps = [
        ":nfs3",
        ":nfs4",
        "//third_party/gtest:gtest_main",
    ],
    linkstatic = 1,
)
