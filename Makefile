lint:
	staticcheck ./...

clean:
	-rm ddbrew

build:
	go build ./cmd/ddbrew/ddbrew.go

test:
	go test

all: lint test

.PHONY: lint build clean test all
