cc_binary(
    name = "fscli",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp", "*.h", "commands/*.cpp"]),
    deps = [
        "//filesys",
        "//third_party/glog",
        "//third_party/gflags"
    ],
    linkstatic = 1
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
