.PHONY: bins clean setup test test-race thriftc

SHELL = /bin/bash

TEST_ENV=CHERAMI_ENVIRONMENT=test CHERAMI_CONFIG_DIR=`pwd`/config

PROJECT_ROOT=github.com/uber/cherami-server
export GO15VENDOREXPERIMENT=1
NOVENDOR = $(shell GO15VENDOREXPERIMENT=1 glide novendor)

export PATH := $(GOPATH)/bin:$(PATH)

THRIFT_GENDIR=.generated

THRIFT_DIR = idl/cherami
THRIFT_INTERNAL_DIR = idl/cherami_internal

THRIFT_SRCS = $(THRIFT_INTERNAL_DIR)/metadata.thrift \
	      $(THRIFT_INTERNAL_DIR)/controller.thrift \
	      $(THRIFT_INTERNAL_DIR)/admin.thrift \
	      $(THRIFT_INTERNAL_DIR)/store.thrift \
	      $(THRIFT_INTERNAL_DIR)/replicator.thrift \
	      $(THRIFT_DIR)/cherami.thrift

export CGO_CFLAGS=$(env CGO_FLAGS) -I$(CURDIR)/rocksdb/include
export CGO_LDFLAGS=$(env CGO_LDFLAGS) -L$(CURDIR)/rocksdb/lib -lrocksdb

export CHERAMI_STORE=$(shell dirname `mktemp -u store.test.XXX`)/cherami_store

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

THRIFT_GEN=$(GOPATH)/bin/thrift-gen

define thriftrule
THRIFT_GEN_SRC += $(THRIFT_GENDIR)/go/$1/tchan-$1.go

$(THRIFT_GENDIR)/go/$1/tchan-$1.go:: $2 $(THRIFT_GEN)
	@mkdir -p $(THRIFT_GENDIR)/go
	$(ECHO_V)$(THRIFT_GEN) --generateThrift --packagePrefix $(PROJECT_ROOT)/$(THRIFT_GENDIR)/go/ --inputFile $2 --outputDir $(THRIFT_GENDIR)/go \
		$(foreach template,$(THRIFT_TEMPLATES), --template $(template))
endef

$(foreach tsrc,$(THRIFT_SRCS),$(eval $(call \
	thriftrule,$(basename $(notdir \
	$(shell echo $(tsrc) | tr A-Z a-z))),$(tsrc))))


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

thriftc: $(THRIFT_GEN_SRC)

server_dep:
	glide install

bins: server_dep checkrocksdb checkcassandra thriftc
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
	rm -rf .generated
