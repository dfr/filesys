cc_library(
    name = "rocks",
    copts = ["-std=c++14"],
    srcs = glob(["*.cpp"]),
    hdrs = ["rocks.h"],
    deps = [
        "//keyval:hdrs",
        "//third_party/glog",
        "//third_party/rocksdb",
    ],
    visibility = ["//visibility:public"]
)
