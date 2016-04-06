#include <cassert>
#include <memory>

#include <fs++/urlparser.h>
#include <gtest/gtest.h>

using namespace filesys;
using namespace std;
using namespace testing;

struct UrlParserTest: public ::testing::Test
{
    UrlParserTest()
    {
    }
};

TEST_F(UrlParserTest, Hostbased)
{
    UrlParser p("nfs://server:1234/some/path");
    EXPECT_TRUE(p.isHostbased());
    EXPECT_EQ("nfs", p.scheme);
    EXPECT_EQ("server", p.host);
    EXPECT_EQ("1234", p.port);
    EXPECT_EQ("some/path", p.path);
}

TEST_F(UrlParserTest, Pathbased)
{
    UrlParser p("file://some/path");
    EXPECT_TRUE(p.isPathbased());
    EXPECT_EQ("file", p.scheme);
    EXPECT_EQ("some/path", p.path);

    UrlParser p2("file:///some/path");
    EXPECT_EQ("/some/path", p2.path);
}

TEST_F(UrlParserTest, Query)
{
    UrlParser p("file://some/path?foo=bar&bar=baz");
    EXPECT_EQ("some/path", p.path);
    EXPECT_EQ("bar", p.query["foo"]);
    EXPECT_EQ("baz", p.query["bar"]);

    UrlParser p2("file://some/path?foo=bar;bar=baz");
    EXPECT_EQ("some/path", p2.path);
    EXPECT_EQ("bar", p2.query["foo"]);
    EXPECT_EQ("baz", p2.query["bar"]);
}
