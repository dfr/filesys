#! /usr/bin/env bash

. "cli/test/unittest.bash"

rm -rf testenv
mkdir testenv
echo foo > testenv/foo

function test_cat() {
    ./cli/fscli file://testenv > $TEST_log << EOF
chmod 400 foo
ls
EOF
    expect_log "-r--------"
}

run_suite "Tests for chmod command"
