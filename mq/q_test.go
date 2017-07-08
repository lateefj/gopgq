package mq

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	_ "github.com/lib/pq" // Postgresql Driver
)

var db *sql.DB

func init() {
	user := os.Getenv("USER")
	var err error
	db, err = sql.Open("postgres", fmt.Sprintf("postgres://%s:@localhost/pgmq?sslmode=disable", user))
	if err != nil {
		log.Fatal(err)
		return
	}
	db.SetMaxOpenConns(8)
}

func setup() *Pgmq {
	return NewPgmq(db, "test_")
}
func cleanup(mq *Pgmq) {
	mq.DropSchema()
}
func TestSchema(t *testing.T) {
	// t.Fatal("not implemented")
	mq := setup()
	err := mq.CreateSchema()
	if err != nil {
		t.Fatalf("Could not create schema %s", err)
	}
	err = mq.DropSchema()
	if err != nil {
		t.Fatalf("Could not drop schema %s", err)
	}

}
func TestPublishConsume(t *testing.T) {
	// t.Fatal("not implemented")
	mq := setup()
	err := mq.CreateSchema()
	if err != nil {
		t.Fatalf("Could not create schema %s", err)
	}
	defer func() {
		err := mq.DropSchema()
		if err != nil {
			t.Fatalf("Could not drop schema %s", err)
		}
	}()

	topic := "foo"
	messages := []*Message{&Message{Topic: topic, Payload: []byte("test")}}
	err = mq.Publish(messages)
	if err != nil {
		t.Fatalf("Failed to publish %s", err)
	}
	size := len(messages)
	recipts := make(chan *MessageRecipt)
	consumedMessages, err := mq.Consumer(topic, size, recipts)
	if err != nil {
		t.Fatalf("Failed to consumer %s", err)
	}
	count := 0
	for _, m := range consumedMessages {
		count += 1
		recipts <- &MessageRecipt{Id: m.Id, Success: true}
	}
	close(recipts)
	if count != size {
		t.Errorf("Expected %d message however got %d", size, count)
	}
}

func publishConsumeSize(b *testing.B, size int) {
	b.ReportAllocs()

	topic := "test"
	messages := make([]*Message, size)
	for i := 0; i < size; i++ {
		messages[i] = &Message{Topic: topic, Payload: []byte("Testing load capacity of a message queue system written in go using Postgresql RDBMS")}
	}

	mq := setup()
	err := mq.CreateSchema()
	if err != nil {
		b.Fatalf("Could not create schema %s", err)
	}
	defer func() {
		err := mq.DropSchema()
		if err != nil {
			b.Fatalf("Could not drop schema %s", err)
		}
	}()
	for i := 0; i < b.N; i++ {
		err = mq.Publish(messages)
		if err != nil {
			b.Fatalf("Failed to publish %s", err)
		}
		recipts := make(chan *MessageRecipt, size)
		consumedMessages, err := mq.Consumer(topic, size, recipts)
		if err != nil {
			b.Fatalf("Failed to consumer %s", err)
		}
		for _, m := range consumedMessages {
			b.SetBytes(int64(len(m.Payload)))
			recipts <- &MessageRecipt{Id: m.Id, Success: true}
		}
		close(recipts)
	}
}

func BenchmarkPublishConsume1(b *testing.B)     { publishConsumeSize(b, 1) }
func BenchmarkPublishConsume10(b *testing.B)    { publishConsumeSize(b, 10) }
func BenchmarkPublishConsume100(b *testing.B)   { publishConsumeSize(b, 100) }
func BenchmarkPublishConsume1000(b *testing.B)  { publishConsumeSize(b, 1000) }
func BenchmarkPublishConsume10000(b *testing.B) { publishConsumeSize(b, 10000) }
