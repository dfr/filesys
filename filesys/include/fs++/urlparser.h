// -*- c++ -*-
#pragma once

#include <map>
#include <string>
#include <vector>

namespace filesys {

struct UrlParser
{
    UrlParser(const std::string& url);
    bool isHostbased();
    bool isPathbased();
    void parseScheme(std::string& s);
    void parseHost(std::string& s);
    void parseIPv4(std::string& s);
    void parseIPv6(std::string& s);
    void parsePort(std::string& s);
    void parsePath(std::string& s);
    void parseQueryTerm(const std::string& s);

    std::string scheme;
    std::string schemeSpecific;
    std::string host;
    std::string port;
    std::string path;
    std::map<std::string, std::string> query;
};

}
