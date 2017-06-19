#-
# Copyright (c) 2016-present Doug Rabson
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.
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
