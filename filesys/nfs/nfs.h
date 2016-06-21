/*-
 * Copyright (c) 2016 Doug Rabson
 * All rights reserved.
 */

#pragma once

#include <climits>
#include <iomanip>
#include <string>
#include <map>
#include <vector>

#include <fs++/filesys.h>

namespace filesys {
namespace nfs {

class NfsFilesystemFactory: public FilesystemFactory
{
public:
    std::string name() const override { return "nfs"; }
    std::pair<std::shared_ptr<Filesystem>, std::string> mount(
        FilesystemManager* fsman, const std::string& url) override;
};

void init(FilesystemManager* fsman);

}
}
