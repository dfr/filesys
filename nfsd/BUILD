cc_binary(
    name = "nfsd",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp"]),
    deps = [
        ":nfs3",
        "//rpcxx",
        "//filesys",
    ]
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
        "//third_party/gtest:gtest_main",
    ],
    linkstatic = 1,
)
