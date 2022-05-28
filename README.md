*🚧 Work in progress. Not stable. 🚧*

# ddbrew

Simple DynamoDB utility🍺.

## Installation

```bash
go install github.com/shuntaka9576/ddbrew/cmd/ddbrew@latest
```

## Usage

### Backup
Retrieves the records of the table specified in the argument and saves them to a file. The format of the file is jsonl, and if not specified, the file is created with the file name backup_tableName_yyyyymmdd-HHMMSS.jsonl. Use each option to adjust RCU/RRU.

```bash
ddbrew backup fooTable
```

### Restore
Reads the jsonl file and writes it to the table. The RRUs to be consumed can be checked with the `--dry-run` option. To limit the number of writes, use the `--limit` option

```bash
ddbrew restore fooTable \
  --file ./testdata/1.jsonl
```

### Truncate
Read jsonl file and delete table data. Note that the deletion also consumes RUC/RRU.

```bash
ddbrew truncate fooTable \
  --file ./testdata/1.jsonl
```

### Use DynamoDB Local

If you use DynamoDB Local, please use the `--local` option
```bash
ddbrew restore fooTable \
  --file testdata/1.jsonl \
  --local http://localhost:8000
```

