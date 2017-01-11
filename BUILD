#-
# Copyright (c) 2016-2017 Doug Rabson
# All rights reserved.
#

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
        "//filesys/distfs:small",
        "//keyval/paxos:small",
        "//nfsd:small",
        "//util:small",
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
        "//filesys/distfs:distfsck",
        "//external:ldb",
    ],
)

load("//tools/build_defs/pkg:pkg.bzl", "pkg_tar")

pkg_tar(
    name = "dist-bin",
    files = [":distfiles"],
    mode = "0755",
    package_dir = "usr/local/unfsd/bin",
)

pkg_tar(
    name = "dist-scripts",
    files = ["pkg/freebsd/scripts/unfsd"],
    package_dir = "usr/local/etc/rc.d",
)

pkg_tar(
    name = "dist",
    extension = "tar.gz",
    deps = [
        ":dist-bin",
        ":dist-scripts",
        ],
    )
