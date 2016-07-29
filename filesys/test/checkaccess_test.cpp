/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#include <cassert>
#include <memory>

#include <filesys/filesys.h>
#include <gtest/gtest.h>

using namespace filesys;
using namespace std;
using namespace testing;

struct CheckAccessTest: public ::testing::Test
{
    CheckAccessTest()
        : cred99(99, 99, {99, 100, 101}),
          cred100(100, 100, {99, 100, 101}),
          cred101(101, 101, {101}),
          priv(0, 0, {0}, true)
    {
    }

    Credential cred99;
    Credential cred100;
    Credential cred101;
    Credential priv;
};

TEST_F(CheckAccessTest, User)
{
    // User 99 matches owner
    CheckAccess(99, 99, 0700, cred99, AccessFlags::READ);
    CheckAccess(99, 99, 0700, cred99, AccessFlags::WRITE);
    CheckAccess(99, 99, 0700, cred99, AccessFlags::EXECUTE);

    // User 100 matches group and is denied
    EXPECT_THROW(
        CheckAccess(99, 99, 0700, cred100, AccessFlags::READ), system_error);
    EXPECT_THROW(
        CheckAccess(99, 99, 0700, cred100, AccessFlags::WRITE), system_error);
    EXPECT_THROW(
        CheckAccess(99, 99, 0700, cred100, AccessFlags::EXECUTE), system_error);

    // User 99 matches user and is denied
    EXPECT_THROW(
        CheckAccess(99, 99, 0000, cred99, AccessFlags::READ), system_error);
    EXPECT_THROW(
        CheckAccess(99, 99, 0000, cred99, AccessFlags::WRITE), system_error);
    EXPECT_THROW(
        CheckAccess(99, 99, 0000, cred99, AccessFlags::EXECUTE), system_error);
}

TEST_F(CheckAccessTest, Group)
{
    // User 99 fails access since it matches owner
    EXPECT_THROW(
        CheckAccess(99, 99, 0070, cred99, AccessFlags::READ), system_error);
    EXPECT_THROW(
        CheckAccess(99, 99, 0070, cred99, AccessFlags::WRITE), system_error);
    EXPECT_THROW(
        CheckAccess(99, 99, 0070, cred99, AccessFlags::EXECUTE), system_error);

    // User 100 matches the group
    CheckAccess(99, 99, 0070, cred100, AccessFlags::READ);
    CheckAccess(99, 99, 0070, cred100, AccessFlags::WRITE);
    CheckAccess(99, 99, 0070, cred100, AccessFlags::EXECUTE);

    // User 101 doesn't have group 99
    EXPECT_THROW(
        CheckAccess(99, 99, 0070, cred101, AccessFlags::READ), system_error);
    EXPECT_THROW(
        CheckAccess(99, 99, 0070, cred101, AccessFlags::WRITE), system_error);
    EXPECT_THROW(
        CheckAccess(99, 99, 0070, cred101, AccessFlags::EXECUTE), system_error);
}

TEST_F(CheckAccessTest, Other)
{
    // User 99 fails access since it matches owner
    EXPECT_THROW(
        CheckAccess(99, 99, 0007, cred99, AccessFlags::READ), system_error);
    EXPECT_THROW(
        CheckAccess(99, 99, 0007, cred99, AccessFlags::WRITE), system_error);
    EXPECT_THROW(
        CheckAccess(99, 99, 0007, cred99, AccessFlags::EXECUTE), system_error);

    // User 100 fails access since it matches group
    EXPECT_THROW(
        CheckAccess(99, 99, 0007, cred100, AccessFlags::READ), system_error);
    EXPECT_THROW(
        CheckAccess(99, 99, 0007, cred100, AccessFlags::WRITE), system_error);
    EXPECT_THROW(
        CheckAccess(99, 99, 0007, cred100, AccessFlags::EXECUTE), system_error);

    // User 101 matches other
    CheckAccess(99, 99, 0007, cred101, AccessFlags::READ);
    CheckAccess(99, 99, 0007, cred101, AccessFlags::WRITE);
    CheckAccess(99, 99, 0007, cred101, AccessFlags::EXECUTE);
}

TEST_F(CheckAccessTest, Privileged)
{
    // Privileged user matches everything
    CheckAccess(99, 99, 0000, priv, AccessFlags::READ);
    CheckAccess(99, 99, 0000, priv, AccessFlags::WRITE);
    CheckAccess(99, 99, 0000, priv, AccessFlags::EXECUTE);
}
