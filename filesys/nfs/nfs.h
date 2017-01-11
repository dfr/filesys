/*-
 * Copyright (c) 2016-2017 Doug Rabson
 * All rights reserved.
 */

#pragma once

#include <climits>
#include <iomanip>
#include <string>
#include <map>
#include <vector>

#include <filesys/filesys.h>

namespace filesys {
namespace nfs {

class NfsFilesystemFactory: public FilesystemFactory
{
public:
    std::string name() const override { return "nfs"; }
    std::shared_ptr<Filesystem> mount(const std::string& url) override;
};

void init(FilesystemManager* fsman);

}
}
