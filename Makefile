.PHONY: build

SHELL := /bin/bash
BUILD_DATE=`date +%FT%T%z`
GIT_COMMIT=`git rev-parse --short HEAD`
GIT_BRANCH=`git rev-parse --symbolic-full-name --abbrev-ref HEAD`
GIT_DIRTY=`git diff-index --quiet HEAD -- || echo "dirty-"`
VERSION=`git describe --always`
GO_VERSION=`go version | awk '{print $$3}'`
MINIMUM_GO_VERSION=1.16.2
BUILD_FLAGS="-X main.gitCommit=${GIT_COMMIT} -X main.version=${VERSION} -X main.goVersion=${GO_VERSION} -X main.buildDate=${BUILD_DATE}"
CI_BUILD_FLAGS="-w -extldflags "-static" -X main.gitCommit=${GIT_COMMIT} -X main.version=${VERSION} -X main.goVersion=${GO_VERSION} -X main.buildDate=${BUILD_DATE}"

descr:
	"You are building the EOS exporter binary."

build:
	go mod tidy
	go build .
run:
	go run eos_exporter.go

all: descr build
