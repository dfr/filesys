#! /usr/bin/env bash

GIT_DATE_TIME=$(date "+%d/%m/%y %H:%M")
GIT_SHA=$(git rev-parse HEAD 2>nil)

#echo $GIT_DATE_TIME
#echo $GIT_SHA
#exit 0

echo "#include \"util/build_version.h\""
echo "const char* rocksdb_build_git_sha = \"rocksdb_build_git_sha:${GIT_SHA}\";"
echo "const char* rocksdb_build_git_datetime = \"rocksdb_build_git_datetime:${GIT_DATE_TIME}\";"
echo const char* rocksdb_build_compile_date = __DATE__\;
