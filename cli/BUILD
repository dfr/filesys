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

cc_binary(
    name = "fscli",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp", "*.h", "commands/*.cpp"]),
    deps = [
        "//filesys",
        "//external:glog",
        "//external:gflags"
    ],
    linkstatic = 1,
    visibility = ["//visibility:public"],
)

test_suite(
    name = "small",
    tags = ["small"]
)

sh_test(
    name = "cat_test",
    size = "small",
    srcs = ["test/cat_test.sh"],
    deps = [":unittest"],
    data = [":fscli"],
)

sh_test(
    name = "cd_test",
    size = "small",
    srcs = ["test/cd_test.sh"],
    deps = [":unittest"],
    data = [":fscli"],
)

sh_test(
    name = "chmod_test",
    size = "small",
    srcs = ["test/chmod_test.sh"],
    deps = [":unittest"],
    data = [":fscli"],
)

sh_test(
    name = "ln_test",
    size = "small",
    srcs = ["test/ln_test.sh"],
    deps = [":unittest"],
    data = [":fscli"],
)

sh_test(
    name = "mv_test",
    size = "small",
    srcs = ["test/mv_test.sh"],
    deps = [":unittest"],
    data = [":fscli"],
)

sh_test(
    name = "rm_test",
    size = "small",
    srcs = ["test/rm_test.sh"],
    deps = [":unittest"],
    data = [":fscli"],
)

sh_library(
    name = "unittest",
    srcs = ["test/unittest.bash"],
    data = ["test/testenv.sh"]
)
