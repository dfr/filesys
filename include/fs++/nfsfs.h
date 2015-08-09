#pragma once

namespace filesys {

class NfsFilesystemFactory: public FilesystemFactory
{
public:
    std::shared_ptr<Filesystem> mount(const std::string& url) override;
};

}
