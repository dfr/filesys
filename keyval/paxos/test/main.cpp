/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#include <gflags/gflags.h>
#include <gmock/gmock.h>
#include <glog/logging.h>

int main(int argc, char **argv) {
    gflags::AllowCommandLineReparsing();
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging(argv[0]);
    return RUN_ALL_TESTS();
}
