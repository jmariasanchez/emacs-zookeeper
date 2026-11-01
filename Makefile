# Developer entry points. On macOS with Nix toolchains, run `. ./env.sh` first.
EMACS ?= emacs
LISP := $(wildcard lisp/*.el)

.PHONY: all build release test rust-test clippy lisp-compile lisp-test lisp-test-integration compat bench clean

all: build

build:
	cargo build -p zk-bridge

release:
	cargo build --release -p zk-bridge

test: rust-test lisp-test

rust-test:
	cargo test --workspace

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

lisp-compile:
	$(EMACS) -Q --batch -L lisp --eval '(setq byte-compile-error-on-warn t)' -f batch-byte-compile $(LISP)
	rm -f lisp/*.elc

lisp-test:
	EMACS=$(EMACS) sh tests/ert/run.sh

# Starts a throwaway ZooKeeper, runs ERT including the real-bridge flow, then stops it.
lisp-test-integration: build
	@set -e; export ZK_DEV_STATE="$${TMPDIR:-/tmp}/zk-dev-make"; \
	eval "$$(scripts/dev-zk.sh start)"; \
	trap 'scripts/dev-zk.sh stop' EXIT; \
	ZK_BRIDGE="$(CURDIR)/target/debug/zk-bridge" EMACS=$(EMACS) sh tests/ert/run.sh

compat:
	scripts/compat-matrix.sh

bench:
	cargo bench -p zk-bridge

clean:
	cargo clean
	rm -f lisp/*.elc
