#! /usr/bin/env bash

. "cli/test/unittest.bash"

rm -rf testenv
mkdir testenv
echo foo > testenv/foo

function test_simple() {
    ./cli/fscli file://testenv > $TEST_log << EOF
ln foo bar
ls
EOF
    expect_log "bar"
}

function test_symlink {
    ./cli/fscli file://testenv > $TEST_log << EOF
ln -s foo foo2
ls
EOF
    expect_log "foo2 -> foo"
}

function test_target_dir {
    ./cli/fscli file://testenv > $TEST_log << EOF
mkdir dir
ln bar dir
ls dir
EOF
    expect_log "bar"
}

function test_target_dir_symlink {
    ./cli/fscli file://testenv > $TEST_log << EOF
ln -s bar dir/bar2
ls dir
EOF
    expect_log "bar2"
}

run_suite "Tests for ln command"
