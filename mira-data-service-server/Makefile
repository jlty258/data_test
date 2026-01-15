VERSION=v1.2.1
BRANCH=v1.2.1
OS := $(shell uname -s)
PLATFORM=$(shell uname -m)
ARCH := $(shell uname -m)
DATETIME=$(shell date "+%Y/%m/%d %H:%M:%S")
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GIT_COMMIT := $(shell git log --pretty=format:'%h' -n 1)
# Note: Update this path to your cross-compiler, download from https://github.com/messense/homebrew-macos-cross-toolchains/releases
CROSS_BUILD_LINUX_GUN_GCC := /Users/hxy/wkdir/x86_64-unknown-linux-gnu/build/x86_64-unknown-linux-gnu-gcc

LOCALCONF_HOME := chainweaver.org.cn/chainweaver/mira/mira-data-service/config
GOLDFLAGS := -X "${LOCALCONF_HOME}.CurrentVersion=${VERSION}" \
             -X "${LOCALCONF_HOME}.BuildDateTime=${DATETIME}" \
             -X "${LOCALCONF_HOME}.GitBranch=${GIT_BRANCH}" \
             -X "${LOCALCONF_HOME}.GitCommit=${GIT_COMMIT}"

.PHONY: all build build_local build_docker build_docker_simple build_linux clean generate ut build_binary go_mod_tidy_vendor cover_stats

# Build all targets and run tests
all: build build_local build_docker build_docker_simple build_linux ut

# Local build
build: go_mod_tidy_vendor build_binary

# Local build with vendor dependencies
build_local:  build_binary

# Docker build
build_docker: go_mod_tidy_vendor
	./docker_build.sh -t $(VERSION) -p false

# Simple Docker build without tidy and vendor
build_docker_simple: go_mod_tidy_vendor build_binary
	./docker_build.sh -t $(VERSION) -p false -d Dockerfile_simple

build_binary:
	go build -ldflags '$(GOLDFLAGS)' -o dataserver ./server

# Cross compile for Linux
build_linux: go_mod_tidy_vendor
	rm -rf build/linux_amd64
	mkdir -p build/linux_amd64
	CGO_ENABLED=1 CC=$(CROSS_BUILD_LINUX_GUN_GCC) GOARCH=amd64 GOOS=linux go build -ldflags '$(GOLDFLAGS)' -o ./build/linux_amd64/mira-data-service ./server
	md5 ./build/linux_amd64/mira-data-service | awk '{print $NF}' > build/linux_amd64/md5.txt

# Clean build artifacts
clean:
	rm -rf build

# Generate code (no use)
generate:
	go generate ./...

# Unit tests
ut:
	go test ./...

# Helper target to tidy and vendor dependencies
go_mod_tidy_vendor:
	go mod tidy
	go mod vendor

cover_stats: go_mod_tidy_vendor
	./coverstats.sh go
