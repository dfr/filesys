#! /usr/bin/env bash

. "cli/test/unittest.bash"

rm -rf testenv
mkdir testenv
echo foo > testenv/foo

function test_rm() {
    ./cli/fscli file://testenv > $TEST_log << EOF
cp foo bar
rm bar
ls bar
EOF
    expect_log "bar: No such file or directory"
}

function test_rmdir() {
    ./cli/fscli file://testenv > $TEST_log << EOF
mkdir bar
rmdir bar
ls bar
EOF
    expect_log "bar: No such file or directory"
}

run_suite "Tests for rm and rmdir command"
