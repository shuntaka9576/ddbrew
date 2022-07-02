# Integration test

## Before running integration tests

### Build ddbrew to your PATH

```bash
make build
```

## test

```bash
# If already installed with brew
brew uninstall ddbrew
# check test version
ddbrew --version
# required assume role
go test -v ./integration
```

