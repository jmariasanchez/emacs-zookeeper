#!/bin/sh
# Run zookeeper.el ERT tests.  Integration tests run when ZK_BRIDGE and
# ZK_CONNECT are set, and skip otherwise.
cd "$(dirname "$0")/../.." || exit 1
exec "${EMACS:-emacs}" -Q --batch -L lisp -L tests/ert -l ert \
  -l tests/ert/zookeeper-test.el \
  -l tests/ert/zookeeper-integration-test.el \
  -f ert-run-tests-batch-and-exit
