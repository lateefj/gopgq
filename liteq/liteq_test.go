package liteq

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/lateefj/gq"
	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

const (
	testPath = "/tmp/gq_liteq_test.db"
)

func init() {
	var err error
	os.Remove(testPath)

	db, err = sql.Open("sqlite3", testPath)
	if err != nil {
		log.Fatal(err)
	}
}

func setup() *Liteq {
	return NewLiteq(db, "test_")
}
func cleanup(mq *Liteq) {
	mq.Destroy()
}
func TestSchema(t *testing.T) {
	// t.Fatal("not implemented")
	mq := setup()
	err := mq.Create()
	if err != nil {
		t.Fatalf("Could not create schema %s", err)
	}
	err = mq.Destroy()
	if err != nil {
		t.Fatalf("Could not drop schema %s", err)
	}

}

func TestPublishConsume(t *testing.T) {
	mq := setup()
	err := mq.Create()
	if err != nil {
		t.Fatalf("Could not create schema %s", err)
	}
	defer cleanup(mq)

	messages := []*gq.Message{&gq.Message{Payload: []byte("test")}}
	err = mq.Publish(messages)
	if err != nil {
		t.Fatalf("Failed to publish %s", err)
	}
	size := len(messages)
	recipts := make([]*gq.Receipt, size)
	consumedMessages, err := mq.ConsumeBatch(size)
	if err != nil {
		t.Fatalf("Failed to consumer %s", err)
	}
	fmt.Printf("Returned %d messages \n", len(consumedMessages))
	count := 0
	for j, m := range consumedMessages {
		count += 1
		recipts[j] = &gq.Receipt{Id: m.Id, Success: true}
	}
	mq.Commit(recipts)
	if count != size {
		t.Errorf("Expected %d message however got %d", size, count)
	}
	consumedMessages, err = mq.ConsumeBatch(size)
	if err != nil {
		t.Fatalf("Failed to consumer %s", err)
	}
	if len(consumedMessages) != 0 {
		t.Errorf("Failed to have consumed message of 0 was %d", len(consumedMessages))
	}
}

func TestStream(t *testing.T) {
	// t.Fatal("not implemented")
	mq := setup()
	err := mq.Create()
	if err != nil {
		t.Fatalf("Could not create schema %s", err)
	}
	defer func() {
		err := mq.Destroy()
		if err != nil {
			t.Fatalf("Could not drop schema %s", err)
		}
	}()

	messages := []*gq.Message{&gq.Message{Payload: []byte("test")}}
	err = mq.Publish(messages)
	if err != nil {
		t.Fatalf("Failed to publish %s", err)
	}
	size := len(messages)
	stream := make(chan []*gq.ConsumerMessage, 0)
	pause := 10 * time.Millisecond
	go mq.Stream(size, stream, pause)
	count := 0
	recipts := make([]*gq.Receipt, size)
	for group := range stream {
		for _, m := range group {
			recipts[count] = &gq.Receipt{Id: m.Id, Success: true}
			count += 1
		}
		if count >= size {
			mq.Exit()
			close(stream)
		}
	}
	if count != size {
		t.Errorf("Expected %d message however got %d", size, count)
	}
	mq.Commit(recipts)
	consumedMessages, err := mq.ConsumeBatch(size)
	if err != nil {
		t.Fatalf("Failed to consumer %s", err)
	}
	if len(consumedMessages) != 0 {
		t.Errorf("Failed to have consumed message of 0 was %d", len(consumedMessages))
	}
}

func publishConsumeSize(b *testing.B, size int) {
	b.ReportAllocs()

	messages := make([]*gq.Message, size)
	for i := 0; i < size; i++ {
		messages[i] = &gq.Message{Payload: []byte("Testing load capacity of a message queue system written in go using Postgresql RDBMS")}
	}

	mq := setup()
	err := mq.Create()
	if err != nil {
		b.Fatalf("Could not create schema %s", err)
	}
	defer func() {
		err := mq.Destroy()
		if err != nil {
			b.Fatalf("Could not drop schema %s", err)
		}
	}()
	for i := 0; i < b.N; i++ {
		err = mq.Publish(messages)
		if err != nil {
			b.Fatalf("Failed to publish %s", err)
		}
		recipts := make([]*gq.Receipt, size)
		consumedMessages, err := mq.ConsumeBatch(size)
		if err != nil {
			b.Fatalf("Failed to consumer %s", err)
		}
		for j, m := range consumedMessages {
			b.SetBytes(int64(len(m.Payload)))
			recipts[j] = &gq.Receipt{Id: m.Id, Success: true}
		}
		mq.Commit(recipts)
	}
}

func BenchmarkPublishConsume1(b *testing.B)     { publishConsumeSize(b, 1) }
func BenchmarkPublishConsume10(b *testing.B)    { publishConsumeSize(b, 10) }
func BenchmarkPublishConsume100(b *testing.B)   { publishConsumeSize(b, 100) }
func BenchmarkPublishConsume1000(b *testing.B)  { publishConsumeSize(b, 1000) }
func BenchmarkPublishConsume10000(b *testing.B) { publishConsumeSize(b, 10000) }
