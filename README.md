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
# => saved default backup_fooTable_20220522-195226.jsonl

# option(long)
ddbrew backup fooTable \
  --scan-limit-internal 1000 \
  --scan-limit 1  \
  --output ./testdata/1.jsonl \
  --stdout

# option(short)
ddbrew backup fooTable \
  -i 1000  \
  -l 1 \
  -o ./testdata/1.jsonl \
  -s
```

### Restore
Reads the jsonl file and writes it to the table. The RRUs to be consumed can be checked with the `--dry-run(short: -d)` option. Use each option to adjust WCU/WRU.

```bash
ddbrew restore fooTable

# option(long)
ddbrew restore fooTable \
  --filepath ./testdata/1.jsonl \
  --procs 1

# option(short)
ddbrew restore fooTable \
  -f ./testdata/1.jsonl \
  -p 1
```

## Features
* [ ] Controlling the number of writes
* [ ] Visualization of writing status
* [ ] Delivery with brew tap
