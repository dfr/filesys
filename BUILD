test_suite(
    name = "small",
    tags = [
        "small",
    ],
    tests = [
        "//cli:small",
        "//filesys:small",
        "//filesys/pfs:small",
        "//filesys/nfs3:small",
        "//filesys/objfs:small",
        "//nfsd:small",
    ]
)

config_setting(
    name = "darwin",
    values = {"cpu": "darwin"},
    visibility = ["//visibility:public"],
)

config_setting(
    name = "freebsd",
    values = {"cpu": "freebsd"},
    visibility = ["//visibility:public"],
)

filegroup(
    name = "distfiles",
    srcs = [
        "//nfsd:nfsd",
        "//cli:fscli",
        "//filesys/objfs:objfsck",
    ],
)

load("//tools/build_defs/pkg:pkg.bzl", "pkg_tar")

pkg_tar(
    name = "dist",
    extension = "tar.gz",
    files = [":distfiles"],
    mode = "0755",
    package_dir = "usr/local/unfsd/bin",
)
