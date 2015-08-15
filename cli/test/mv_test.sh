#! /usr/bin/env bash

. "cli/test/unittest.bash"

rm -rf testenv
mkdir testenv
echo foo > testenv/foo

function test_simple() {
    ./cli/fscli file://testenv > $TEST_log << EOF
mv foo bar
ls
EOF
    expect_log "bar"
}

function test_target_dir {
    ./cli/fscli file://testenv > $TEST_log << EOF
mkdir foo
mv bar foo
ls foo
EOF
    expect_log "bar"
}

run_suite "Tests for mv command"
