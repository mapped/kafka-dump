# Kafka data backup
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Go Report Card](https://goreportcard.com/badge/github.com/huantt/kafka-dump)](https://goreportcard.com/report/github.com/huantt/kafka-dump)

Kafka dump is a tool to back up and restore your Kafka data.

It helps you reduce the cost of storing the data that you don't need to use right now but can not delete.

In other words, this tool is used to back up and restore `Cold data` for Kafka topics.

## Build from source (Ubuntu / Linux)

This tool depends on [`confluent-kafka-go`](https://github.com/confluentinc/confluent-kafka-go), which is a cgo binding around `librdkafka`. Building therefore requires a C toolchain, and `CGO_ENABLED` must be on (it is on by default).

### Prerequisites

```shell
# Go 1.18+ (https://go.dev/dl/). On Ubuntu you can also use the snap:
sudo snap install go --classic

# C toolchain + librdkafka development headers
sudo apt-get update
sudo apt-get install -y build-essential pkg-config librdkafka-dev git
```

### Clone and build

```shell
git clone https://github.com/mapped/kafka-dump.git
cd kafka-dump
go build -o kafka-dump .
```

This produces a `kafka-dump` binary in the current directory. Verify it:

```shell
./kafka-dump --help
./kafka-dump export --help
```

> If `go build` fails with a linker error about `librdkafka` on a non-amd64 host (e.g. arm64), make sure `librdkafka-dev` is installed and build with the dynamic tag: `go build -tags dynamic -o kafka-dump .`

### Run

Invoke the binary directly (all the flags below are documented under [Use command line](#use-command-line)). For example, exporting a protobuf topic to a Parquet file with the value column stored as binary:

```shell
./kafka-dump export \
--storage=file \
--file=/path/to/output/data.parquet \
--value-format=bytes \
--kafka-topics=my-protobuf-topic \
--kafka-group-id=kafka-dump.local \
--kafka-servers=localhost:9092 \
--kafka-username=admin \
--kafka-password=admin \
--kafka-security-protocol=SASL_SSL \
--kafka-sasl-mechanism=PLAIN
```

## Use command line
### Install
```shell
go install github.com/huantt/kafka-dump@latest
```
```shell
export PATH=$PATH:$(go env GOPATH)/bin
```
### Export Kafka topics to parquet file
#### Options
```shell
Usage:
   export [flags]

Flags:
      --concurrent-consumers int                  Number of concurrent consumers (default 1)
      --fetch-message-max-bytes int               Maximum number of bytes per topic+partition to request when fetching messages from the broker. (default 1048576)
  -f, --file string                               Output file path (required)
      --gcs-bucket string                         Google Cloud Storage bucket name
      --gcs-project-id string                     Google Cloud Storage Project ID
      --google-credentials string                 Path to Google Credentials file
  -h, --help                                      help for export
      --kafka-group-id string                     Kafka consumer group ID
      --kafka-password string                     Kafka password
      --kafka-sasl-mechanism string               Kafka password
      --kafka-security-protocol string            Kafka security protocol
      --kafka-servers string                      Kafka servers string
      --kafka-topics stringArray                  Kafka topics
      --kafka-username string                     Kafka username
      --limit uint                                Supports file splitting. Files are split by the number of messages specified
      --max-waiting-seconds-for-new-message int   Max waiting seconds for new message, then this process will be marked as finish. Set -1 to wait forever. (default 30)
      --queued-max-messages-kbytes int            Maximum number of kilobytes per topic+partition in the local consumer queue. This value may be overshot by fetch.message.max.bytes (default 128000)
      --ssl-ca-location string                    location of client ca cert file in pem
      --ssl-certificate-location string           client's certificate location
      --ssl-key-location string                   path to ssl private key
      --ssl-key-password string                   password for ssl private key passphrase
      --storage string                            Storage type: local file (file) or Google cloud storage (gcs) (default "file")
      --value-format string                       Logical type of the Parquet 'value' column (required): "string" for UTF8 text/JSON topics, "bytes" for binary payloads such as protobuf/avro

Global Flags:
      --log-level string   Log level (default "info")
```

> **`--value-format` (required):** Kafka message values are written to a single Parquet `value` column, and a Parquet column has one type for the whole file — so you must declare how the payload should be typed:
> - `--value-format=string` — annotates the column as UTF8 (STRING). Use for text/JSON topics that you want to query as strings.
> - `--value-format=bytes` — writes a raw binary `BYTE_ARRAY` (surfaces as BINARY/BLOB/VARBINARY in DuckDB, Athena, Trino, Spark). Use for binary payloads such as **protobuf** or **avro**. Choosing `string` for binary data leaves it annotated as UTF8, which query engines will mangle as invalid UTF-8.
>
> The physical bytes stored are identical either way; only the column's logical type differs, so `import` round-trips correctly regardless of the format used at export.

#### Sample

- Connect to Kafka cluster without the SSL encryption being enabled for exporting the data.
```shell
kafka-dump export \
--storage=file
--file=path/to/output/data.parquet \
--value-format=string \
--kafka-topics=users-activities \
--kafka-group-id=id=kafka-dump.local \
--kafka-servers=localhost:9092 \
--kafka-username=admin \
--kafka-password=admin \
--kafka-security-protocol=SASL_SSL \
--kafka-sasl-mechanism=PLAIN
```

- Connect to Kafka cluster with the SSL encryption being enabled for exporting the data (using `--value-format=bytes` for a binary/protobuf topic).
```shell
kafka-dump export \
--storage=file
--file=path/to/output/data.parquet \
--value-format=bytes \
--kafka-topics=users-activities \
--kafka-group-id=id=kafka-dump.local \
--kafka-servers=localhost:9092 \
--kafka-username=admin \
--kafka-password=admin \
--kafka-security-protocol=SSL \
--kafka-sasl-mechanism=PLAIN
--ssl-ca-location=<path to ssl cacert>
--ssl-certificate-location=<path to ssl cert>
--ssl-key-location=<path to ssl key>
--ssl-key-password=<ssl key password>
```

### Import Kafka topics from parquet file
```shell
Usage:
   import [flags]

Flags:
  -f, --file string                       Output file path (required)
  -h, --help                              help for import
  -i, --include-partition-and-offset      to store partition and offset of kafka message in file
      --queue-buffering-max-messages      queue buffering max messages (default 10000)
      --kafka-password string             Kafka password
      --kafka-sasl-mechanism string       Kafka password
      --kafka-security-protocol string    Kafka security protocol
      --kafka-servers string              Kafka servers string
      --kafka-username string             Kafka username
      --ssl-ca-location string            location of client ca cert file in pem
      --ssl-certificate-location string   client's certificate location
      --ssl-key-location string           path to ssl private key
      --ssl-key-password string           password for ssl private key passphrase

Global Flags:
      --log-level string   Log level (default "info")
```
#### Sample

- Connect to Kafka cluster without the SSL encryption being enabled for importing the data.
```shell
kafka-dump import \
--file=path/to/input/data.parquet \
--kafka-servers=localhost:9092 \
--kafka-username=admin \
--kafka-password=admin \
--kafka-security-protocol=SASL_SSL \
--kafka-sasl-mechanism=PLAIN
```

- Connect to Kafka cluster with the SSL encryption being enabled for importing the data.
```shell
kafka-dump import \
--file=path/to/input/data.parquet \
--kafka-servers=localhost:9092 \
--kafka-username=admin \
--kafka-password=admin \
--kafka-security-protocol=SSL \
--kafka-sasl-mechanism=PLAIN
--ssl-ca-location=<path to ssl cacert>
--ssl-certificate-location=<path to ssl cert>
--ssl-key-location=<path to ssl key>
--ssl-key-password=<ssl key password>
```

### Stream messages topic to topic
```shell
Usage:
   stream [flags]

Flags:
      --from-kafka-group-id string                Kafka consumer group ID
      --from-kafka-password string                Source Kafka password
      --from-kafka-sasl-mechanism string          Source Kafka password
      --from-kafka-security-protocol string       Source Kafka security protocol
      --from-kafka-servers string                 Source Kafka servers string
      --from-kafka-username string                Source Kafka username
      --from-topic string                         Source topic
      -h, --help                                      help for stream
      --max-waiting-seconds-for-new-message int   Max waiting seconds for new message, then this process will be marked as finish. Set -1 to wait forever. (default 30)
      --to-kafka-password string                  Destination Kafka password
      --to-kafka-sasl-mechanism string            Destination Kafka password
      --to-kafka-security-protocol string         Destination Kafka security protocol
      --to-kafka-servers string                   Destination Kafka servers string
      --to-kafka-username string                  Destination Kafka username
      --to-topic string                           Destination topic

Global Flags:
      --log-level string   Log level (default "info")

```

#### Sample
```shell
kafka-dump stream \
--from-topic=users \
--from-kafka-group-id=stream \
--from-kafka-servers=localhost:9092 \
--from-kafka-username=admin \
--from-kafka-password=admin \
--from-kafka-security-protocol=SASL_SSL \
--from-kafka-sasl-mechanism=PLAIN \
--to-topic=new-users \
--to-kafka-servers=localhost:9092 \
--to-kafka-username=admin \
--to-kafka-password=admin \
--to-kafka-security-protocol=SASL_SSL \
--to-kafka-sasl-mechanism=PLAIN
--max-waiting-seconds-for-new-message=-1
```

### Count number of rows in parquet file
```shell
Usage:
   count-parquet-rows [flags]

Flags:
  -f, --file string   File path (required)
  -h, --help          help for count-parquet-rows

Global Flags:
      --log-level string   Log level (default "info")
```
#### Sample
```shell
kafka-dump count-parquet-rows \
--file=path/to/output/data.parquet
```

## Use Docker
```shell
docker run -d --rm \
-v /local-data:/data \
huanttok/kafka-dump:latest \
kafka-dump export \
--file=/data/path/to/output/data.parquet \
--kafka-topics=users-activities \
--kafka-group-id=id=kafka-dump.local \
--kafka-servers=localhost:9092 \
--kafka-username=admin \
--kafka-password=admin \
--kafka-security-protocol=SASL_SSL \
--kafka-sasl-mechanism=PLAIN
```

## TODO
- Import topics from multiple files or directory
- Import topics from Google Cloud Storage files or directory
