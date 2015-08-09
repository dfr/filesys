cc_binary(
    name = "fscli",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp", "commands/*.cpp"]),
    deps = [
        "//src/nfs:nfs",
        "//include:hdrs",
        "//rpcxx",
        "//third_party/glog",
        "//third_party/gflags"
    ],
    linkstatic = 1
)

cc_library(
    name = "commands",
    srcs = glob(["commands/*.cpp"]),
    hdrs = ["command.h"],
    deps = [
        "//include:hdrs"
    ],
    linkstatic = 1
)
