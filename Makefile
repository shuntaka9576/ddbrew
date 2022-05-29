BIN := ddbrew
VERSION := $(shell git describe --tags)
REVISION := $(shell git rev-parse --short HEAD)

build:
	go build -ldflags="-s -w -X github.com/shuntaka9576/$(BIN)/cli.Version=$(VERSION) -X github.com/shuntaka9576/$(BIN)/cli.Revision=$(REVISION)" -o ddbrew ./cmd/$(BIN)

.PHONY: build
