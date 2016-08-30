cc_library(
    name = "paxos",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp"]) + [":paxosproto", ":kvproto"],
    hdrs = ["paxos.h"],
    deps = [
        "//keyval:hdrs",
        "//util",
    ],
    visibility = ["//visibility:public"]
)

test_suite(
    name = "small",
    tags = ["small"]
)

cc_test(
    name = "paxos_test",
    size = "small",
    copts = ["-std=c++14"],
    srcs = glob(["test/*.cpp", "test/*.h"]),
    deps = [
        ":paxos",
        "//keyval",
        "//external:gmock",
        "//external:rpcxx",
    ],
)

genrule(
    name = "paxosproto",
    srcs = ["paxosproto.x"],
    outs = ["paxosproto.h"],
    tools = ["//external:rpcgen"],
    cmd = "$(location //external:rpcgen) -txics -n keyval::paxos $< > $@",
)

genrule(
    name = "kvproto",
    srcs = ["kvproto.x"],
    outs = ["kvproto.h"],
    tools = ["//external:rpcgen"],
    cmd = "$(location //external:rpcgen) -txics -n keyval::paxos $< > $@",
)
