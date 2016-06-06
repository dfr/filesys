/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <glog/logging.h>

DEFINE_int32(iosize, 65536, "maximum size for read or write requests");
DEFINE_int32(grace_time, 120, "NFSv4 grace period time in seconds");
DEFINE_int32(lease_time, 120, "NFSv4 lease time in seconds");
DEFINE_string(realm, "", "Local krb5 realm name");

int main(int argc, char **argv) {
    gflags::AllowCommandLineReparsing();
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging(argv[0]);
    return RUN_ALL_TESTS();
}
