# Integration test

## Before running integration tests

### Build ddbrew to your PATH

```bash
make build
```

### Deploy DynamoDB tables for testing

```bash
# required assume role
aws cloudformation deploy \
  --template ./integration/cfn/table.yml \
  --stack-name ddbrewTest
```

## test

```bash
# required assume role
go test -v ./integration
```

