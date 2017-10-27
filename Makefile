.PHONY: bins clean setup test test-race cover cover_ci cover_profile

SHELL = /bin/bash

PROJECT_ROOT=github.com/uber/cherami-server
INTEG_TEST_ROOT=./test
TOOLS_ROOT=./tools
export GO15VENDOREXPERIMENT=1
NOVENDOR = $(shell GO15VENDOREXPERIMENT=1 glide novendor)
TEST_ARG ?= -race -v -timeout 5m
TEST_NO_RACE_ARG ?= -timeout 5m
BUILD := ./build
PWD = $(shell pwd)

export PATH := $(GOPATH)/bin:$(PATH)

export CHERAMI_STORE=$(shell dirname `mktemp -u store.test.XXX`)/cherami_store
export CHERAMI_CONFIG_DIR=$(CURDIR)/config

# Determine whether to use embedded rocksdb. This is recommended
# for building the executable for deployment because the binary
# will link statically with rocksdb. Testing with embedded
# rocksdb will be slow to build. If EMBEDROCKSDB=0, user need to
# supply proper CGO_CFLAGS, CGO_LDFLAGS, LD_LIBRARY_PATH value
# in environment variable.
ifneq ($(EMBEDROCKSDB), 0)
	EMBED = -tags=embed
endif

# Automatically gather all srcs
ALL_SRC := $(shell find . -name "*.go" | grep -v -e Godeps -e vendor \
	-e ".*/\..*" \
	-e ".*/_.*" \
	-e ".*/mocks.*")

# all directories with *_test.go files in them
ALL_TEST_DIRS := $(sort $(dir $(filter %_test.go,$(ALL_SRC))))
# all tests other than integration test fall into the pkg_test category
PKG_TEST_DIRS := $(filter-out $(INTEG_TEST_ROOT)%,$(ALL_TEST_DIRS))
PKG_TEST_DIRS := $(filter-out $(TOOLS_ROOT)%,$(PKG_TEST_DIRS))
# dirs that contain integration tests, these need to be treated
# differently to get correct code coverage
INTEG_TEST_DIRS := $(filter $(INTEG_TEST_ROOT)%,$(ALL_TEST_DIRS))

# Need the following option to have integration tests
# count towards coverage. godoc below:
# -coverpkg pkg1,pkg2,pkg3
#   Apply coverage analysis in each test to the given list of packages.
#   The default is for each test to analyze only the package being tested.
#   Packages are specified as import paths.
GOCOVERPKG_ARG := -coverpkg="$(PROJECT_ROOT)/common/...,$(PROJECT_ROOT)/services/...,$(PROJECT_ROOT)/clients/..."

test: lint bins
	@for dir in $(ALL_TEST_DIRS); do \
		go test $(EMBED) "$$dir" $(TEST_NO_RACE_ARG) $(shell glide nv); \
	done;

test-race: $(ALL_SRC)
	@for dir in $(ALL_TEST_DIRS); do \
		go test $(EMBED) "$$dir" $(TEST_ARG) | tee -a "$$dir"_test.log; \
	done;	       

checkcassandra:
	@if ! which cqlsh | grep -q /; then \
		echo "cqlsh not in PATH. please install cassandra and cqlsh" >&2; \
		exit 1; \
	fi

vendor/glide.updated: glide.lock glide.yaml
	glide install
	touch vendor/glide.updated

DEPS = vendor/glide.updated $(ALL_SRC)

cherami-server: $(DEPS)
	go build -i $(EMBED) -o cherami-server cmd/standalone/main.go

cherami-replicator-server: $(DEPS)
	go build -i $(EMBED) -o cherami-replicator-server cmd/replicator/main.go

cherami-cli: $(DEPS)
	go build -i -o cherami-cli cmd/tools/cli/main.go

cherami-admin: $(DEPS)
	go build -i -o cherami-admin cmd/tools/admin/main.go

cherami-replicator-tool: $(DEPS)
	go build -i -o cherami-replicator-tool cmd/tools/replicator/main.go

cherami-cassandra-tool: $(DEPS)
	go build -i -o cherami-cassandra-tool cmd/tools/cassandra/main.go

cherami-store-tool: $(DEPS)
	go build -i $(EMBED) -o cherami-store-tool cmd/tools/store/main.go

cdb: $(DEPS)
	go build -i $(EMBED) -o cdb cmd/tools/cdb/*.go

cmq: $(DEPS)
	go build -i $(EMBED) -o cmq cmd/tools/cmq/*.go

bins: cherami-server cherami-replicator-server cherami-cli cherami-admin cherami-replicator-tool cherami-cassandra-tool cherami-store-tool cdb cmq

cover_profile: lint bins
	@mkdir -p $(BUILD)
	@echo "mode: atomic" > $(BUILD)/cover.out

	@echo Running integration tests:
	@time for dir in $(INTEG_TEST_DIRS); do \
		mkdir -p $(BUILD)/"$$dir"; \
		go test $(EMBED) "$$dir" $(TEST_ARG) $(GOCOVERPKG_ARG) -coverprofile=$(BUILD)/"$$dir"/coverage.out || exit 1; \
		cat $(BUILD)/"$$dir"/coverage.out | grep -v "mode: atomic" >> $(BUILD)/cover.out; \
	done

	@echo Running tests:
	@time for dir in $(PKG_TEST_DIRS); do \
		mkdir -p $(BUILD)/"$$dir"; \
		go test $(EMBED) "$$dir" $(TEST_ARG) -coverprofile=$(BUILD)/"$$dir"/coverage.out || exit 1; \
		cat $(BUILD)/"$$dir"/coverage.out | grep -v "mode: atomic" >> $(BUILD)/cover.out; \
	done

cover: cover_profile
	go tool cover -html=$(BUILD)/cover.out

cover_ci: cover_profile
	goveralls -coverprofile=$(BUILD)/cover.out -service=travis-ci || echo -e "\x1b[31mCoveralls failed\x1b[m"

clean:
	rm -f cherami-server cherami-replicator-server cherami-cli cherami-admin cherami-replicator-tool cherami-cassandra-tool cherami-store-tool cdb cmq
	rm -Rf vendor/*
	rm -Rf $(BUILD)

lint:
	@lintFail=0; for file in $(ALL_SRC); do \
		golint -set_exit_status "$$file"; \
		if [ $$? -eq 1 ]; then lintFail=1; fi; \
	done; \
	if [ $$lintFail -eq 1 ]; then exit 1; fi;
	@OUTPUT=`gofmt -l $(ALL_SRC) 2>&1`; \
	if [ "$$OUTPUT" ]; then \
		echo "Run 'make fmt'. gofmt must be run on the following files:"; \
		echo "$$OUTPUT"; \
		exit 1; \
	fi
	@go tool vet -all -printfuncs=Info,Infof,Debug,Debugf,Warn,Warnf,Panic,Panicf $(ALL_TEST_DIRS)

fmt:
	@gofmt -w $(ALL_SRC)

