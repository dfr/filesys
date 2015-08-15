#! /usr/bin/env bash

. "cli/test/unittest.bash"

rm -rf testenv
mkdir testenv
echo foo > testenv/foo

function test_cat() {
    echo 'cat foo' | ./cli/fscli file://testenv > $TEST_log
    expect_log "foo"
}

run_suite "Tests for cat command"
