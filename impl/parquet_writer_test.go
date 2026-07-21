package impl

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
)

func TestMain(m *testing.M) {
	// The global logger is normally built in main(); initialise it so the
	// log.Info call in Flush does not dereference a nil logger.
	log.Config{Level: "info", Format: "text"}.Build()
	os.Exit(m.Run())
}

// binaryPayload is deliberately not valid UTF-8 (mimics a protobuf message).
var binaryPayload = []byte{0x08, 0x96, 0x01, 0xff, 0xfe, 0x00, 0x80}

func writeOneMessage(t *testing.T, path string, rawValue bool, value []byte) {
	t.Helper()
	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		t.Fatalf("NewLocalFileWriter: %v", err)
	}
	pw, err := NewParquetWriter(fw, rawValue)
	if err != nil {
		t.Fatalf("NewParquetWriter: %v", err)
	}
	topic := "test-topic"
	msg := kafka.Message{
		Value: value,
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    kafka.Offset(1),
		},
		Key:           []byte("k"),
		Timestamp:     time.Now().Truncate(time.Second),
		TimestampType: kafka.TimestampCreateTime,
	}
	if err := pw.Write(msg); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := pw.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

// valueColumnConvertedType returns the pointer to the ConvertedType of the
// "value" column in the written file (nil when the column has no annotation).
func valueColumnConvertedType(t *testing.T, path string) *parquet.ConvertedType {
	t.Helper()
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		t.Fatalf("NewLocalFileReader: %v", err)
	}
	pr, err := reader.NewParquetReader(fr, new(ParquetMessage), 4)
	if err != nil {
		t.Fatalf("NewParquetReader: %v", err)
	}
	defer pr.ReadStop()
	defer fr.Close()
	for _, el := range pr.Footer.Schema {
		if strings.EqualFold(el.Name, "value") {
			return el.ConvertedType
		}
	}
	t.Fatal("value column not found in schema")
	return nil
}

func readOneValue(t *testing.T, path string) []byte {
	t.Helper()
	pr, err := NewParquetReader(path, true)
	if err != nil {
		t.Fatalf("NewParquetReader: %v", err)
	}
	msg := <-pr.Read()
	return msg.Value
}

// TestParquetWriter_BytesFormat_PreservesBinaryValue is the core regression:
// a protobuf-like (non-UTF-8) value must round-trip byte-for-byte and the
// column must carry no UTF8 annotation, so query engines treat it as BLOB.
func TestParquetWriter_BytesFormat_PreservesBinaryValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bytes.parquet")
	writeOneMessage(t, path, true /* rawValue */, binaryPayload)

	if ct := valueColumnConvertedType(t, path); ct != nil {
		t.Errorf("value column should have no convertedtype in bytes mode, got %v", ct)
	}

	got := readOneValue(t, path)
	if string(got) != string(binaryPayload) {
		t.Errorf("value not preserved: got % x, want % x", got, binaryPayload)
	}
}

// TestParquetWriter_StringFormat_KeepsUTF8 asserts the default text path is
// unchanged: the value column stays annotated as UTF8.
func TestParquetWriter_StringFormat_KeepsUTF8(t *testing.T) {
	path := filepath.Join(t.TempDir(), "string.parquet")
	writeOneMessage(t, path, false /* rawValue */, []byte(`{"hello":"world"}`))

	ct := valueColumnConvertedType(t, path)
	if ct == nil || *ct != parquet.ConvertedType_UTF8 {
		t.Errorf("value column should be UTF8 in string mode, got %v", ct)
	}

	got := readOneValue(t, path)
	if string(got) != `{"hello":"world"}` {
		t.Errorf("value not preserved: got %q", string(got))
	}
}
