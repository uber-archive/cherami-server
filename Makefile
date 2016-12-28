.PHONY: bins clean setup test test-race thriftc

SHELL = /bin/bash

PROJECT_ROOT=github.com/uber/cherami-server
export GO15VENDOREXPERIMENT=1
NOVENDOR = $(shell GO15VENDOREXPERIMENT=1 glide novendor)

export PATH := $(GOPATH)/bin:$(PATH)

export CGO_CFLAGS=$(env CGO_FLAGS) -I$(CURDIR)/rocksdb/include
export CGO_LDFLAGS=$(env CGO_LDFLAGS) -L$(CURDIR)/rocksdb/lib -lrocksdb

export CHERAMI_STORE=$(shell dirname `mktemp -u store.test.XXX`)/cherami_store
export CHERAMI_CONFIG_DIR=$(CURDIR)/config

ifeq ($(shell uname -s),Linux)
        export CGO_LDFLAGS=$(env CGO_LDFLAGS) -L$(CURDIR)/rocksdb/lib -lrocksdb -lsnappy -lgflags -lbz2 -lstdc++
        export LD_LIBRARY_PATH=$(CURDIR)/rocksdb/lib:$(env LD_LIBRARY_PATH)
endif

# Automatically gather all srcs
ALL_SRC := $(shell find . -name "*.go" | grep -v -e Godeps -e vendor \
	-e ".*/\..*" \
	-e ".*/_.*" \
	-e ".*/mocks.*")

# all directories with *_test.go files in them
TEST_DIRS := $(sort $(dir $(filter %_test.go,$(ALL_SRC))))

test: bins
	@for dir in $(TEST_DIRS); do \
		go test "$$dir" | tee -a test.log; \
	done;

test-race:
	@for dir in $(TEST_DIRS); do \
		go test -race "$$dir" | tee -a "$$dir"_test.log; \
	done;	       

checkrocksdb:
	@if [ ! -d $(CURDIR)/rocksdb ]; then \
		echo "please install rocksdb and copy the static libraries and include files within $(CURDIR)/rocksdb directory" >&2; \
		echo "one can follow instructions here to build rocksdb: https://github.com/facebook/rocksdb/blob/master/INSTALL.md" >&2; \
		echo "once the repo is cloned and libraries are built, please copy the liborcksdb* to $(CURDIR)/rocksdb/lib and rocksdb/include to $(CURDIR)/rocksdb/include" >&2; \
		exit 1; \
	fi

checkcassandra:
	@if ! which cqlsh | grep -q /; then \
		echo "cqlsh not in PATH. please install cassandra and cqlsh" >&2; \
		exit 1; \
	fi

server_dep:
	glide install

bins: server_dep checkrocksdb checkcassandra
	go build -i -o cmd/standalone/standalone cmd/standalone/main.go
	go build -i -o cmd/replicator/replicator cmd/replicator/main.go
	go build -i -o cmd/tools/cli/cherami-cli cmd/tools/cli/main.go
	go build -i -o cmd/tools/admin/cherami-admin cmd/tools/admin/main.go
	go build -i -o cmd/tools/replicator/cherami-replicator-tool cmd/tools/replicator/main.go
	go build -i -o cmd/tools/cassandra/cherami-cassandra-tool cmd/tools/cassandra/main.go

clean:
	rm -f cmd/standalone/standalone
	rm -f cmd/replicator/replicator
	rm -f cmd/tools/cli/cherami-cli
	rm -f cmd/tools/admin/cherami-admin
	rm -f cmd/tools/replicator/cherami-replicator-tool
	rm -f cmd/tools/cassandra/cherami-cassandra-tool
