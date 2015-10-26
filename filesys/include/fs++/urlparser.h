#pragma once

#include <string>
#include <vector>

namespace filesys {

struct UrlParser
{
    UrlParser(const std::string& url)
    {
        std::string s = url;
        parseScheme(s);
        schemeSpecific = s;
        if (isHostbased()) {
            if (s.substr(0, 2) != "//")
                throw std::runtime_error("malformed url");
            s = s.substr(2);
            parseHost(s);
            if (s[0] == ':') {
                s = s.substr(1);
                parsePort(s);
            }
            path = s;
        }
        else if (scheme == "file") {
            if (s.substr(0, 2) != "//")
                throw std::runtime_error("malformed url");
            path = s.substr(2);
        }
        else if (scheme == "objfs") {
            if (s.substr(0, 2) != "//")
                throw std::runtime_error("malformed url");
            path = s.substr(2);
        }
    }

    bool isHostbased()
    {
        return scheme == "tcp" || scheme == "udp" || scheme == "http" ||
            scheme == "https" || scheme == "nfs";
    }

    void parseScheme(std::string& s)
    {
        scheme = "";
        if (!std::isalpha(s[0]))
            throw std::runtime_error("malformed url");
        while (s.size() > 0 && s[0] != ':') {
            if (!std::isalpha(s[0]))
                throw std::runtime_error("malformed url");
            scheme += s[0];
            s = s.substr(1);
        }
        if (s.size() == 0 || s[0] != ':')
            throw std::runtime_error("malformed url");
        s = s.substr(1);
    }

    void parseHost(std::string& s)
    {
        if (s.size() == 0)
            return;
        if (std::isdigit(s[0])) {
            parseIPv4(s);
        }
        else if (s[0] == '[') {
            parseIPv6(s);
        }
        else {
            int i = 0;
            while (i < s.size() && s[i] != ':' && s[i] != '/')
                i++;
            host = s.substr(0, i);
            s = s.substr(i);
        }
    }

    void parseIPv4(std::string& s)
    {
        host = "";
        for (int i = 0; i < 4; i++) {
            if (s.size() == 0)
                throw std::runtime_error("malformed IPv4 address");
            if (i > 0) {
                if (s[0] != '.')
                    throw std::runtime_error("malformed IPv4 address");
                host += '.';
                s = s.substr(1);
                if (s.size() == 0)
                    throw std::runtime_error("malformed IPv4 address");
            }
            while (s.size() > 0 && std::isdigit(s[0])) {
                host += s[0];
                s = s.substr(1);
            }
        }
    }

    void parseIPv6(std::string& s)
    {
        std::vector<std::uint16_t> parts;

        host = '[';
        auto i = s.find(']');
        if (i == std::string::npos)
            throw std::runtime_error("malformed IPv6 address");
        auto t = s.substr(1, i - 1);
        s = s.substr(i + 1);
        while (t.size() > 0 &&
               (std::isxdigit(t[0]) || t[0] == ':' || t[0] == '.')) {
            host += t[0];
            t = t.substr(1);
        }
        if (t.size() != 0)
            throw std::runtime_error("malformed IPv6 address");
        host += ']';
    }

    void parsePort(std::string& s)
    {
        port = "";
        while (s.size() > 0 && std::isdigit(s[0])) {
            port += s[0];
            s = s.substr(1);
        }
    }

    std::string scheme;
    std::string schemeSpecific;
    std::string host;
    std::string port;
    std::string path;
};

}
