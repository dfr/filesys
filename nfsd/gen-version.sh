#! /bin/sh

echo '#include "nfsd/version.h"'
echo 'namespace nfsd {'
echo 'namespace version {'
echo const char* commit = '"'`git show --no-patch --date=rfc --format='%h %cd' HEAD`'";'
echo std::time_t date = `git show --no-patch --format='%ct' HEAD`';'
echo '}'
echo '}'
