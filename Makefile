BIN := ddbrew
BINPATH := $(GOPATH)/bin
DISTPATH := ./dist/ddbrew_darwin_amd64_v1/$(BIN)

clean:
	-rm $(BINPATH)/$(BIN)
build: clean
	goreleaser release --snapshot --rm-dist --skip-publish
	cp $(DISTPATH) $(GOPATH)/bin

.PHONY: build clean
