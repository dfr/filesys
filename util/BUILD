cc_inc_library(
    name = "util",
    hdrs = glob(["include/*.h"]),
    prefix = "include",
    visibility = ["//visibility:public"]
)

test_suite(
    name = "small",
    tags = ["small"]
)

cc_test(
    name = "util_test",
    size = "small",
    copts = ["-std=c++14"],
    srcs = glob(["test/*.cpp"]),
    deps = [
        ":util",
        "//external:gmock_main",
    ],
    linkopts = select({
        "//:freebsd": ["-pthread", "-lm"],
        "//:darwin": [],
    }),
    linkstatic = 1,
)
