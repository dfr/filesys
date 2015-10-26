test_suite(
    name = "small",
    tags = [
        "small",
    ],
    tests = [
        "//cli:small",
        "//filesys/pfs:small",
        "//filesys/nfs:small",
        "//filesys/objfs:small",
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
