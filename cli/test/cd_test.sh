#! /usr/bin/env bash

. "cli/test/unittest.bash"

rm -rf testenv
mkdir testenv
mkdir testenv/foo
echo foo > testenv/foo/bar

function test_simple() {
    ./cli/fscli file://testenv > $TEST_log << EOF
cd foo
ls
EOF
    expect_log "bar"
}

run_suite "Tests for cd command"
