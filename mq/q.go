package mq

import (
	"database/sql"
	"fmt"

	pq "github.com/lib/pq" // Postgresql Driver
)

var createSchema = `
CREATE SEQUENCE IF NOT EXISTS %sq_id_seq;
CREATE TABLE IF NOT EXISTS %sq (
	id INT8 NOT NULL DEFAULT nextval('%sq_id_seq') PRIMARY KEY,
	timestamp TIMESTAMP NOT NULL DEFAULt now(),
	topic TEXT,
	payload BYTEA
);
CREATE INDEX IF NOT EXISTS %sq_timestamp_idx ON %sq (topic, timestamp ASC);
`
var dropScrema = `
DROP TABLE IF EXISTS %sq;
DROP SEQUENCE IF EXISTS %sq_id_seq;
`

// Message ... Basic message
type Message struct {
	Topic   string
	Payload []byte
}

// ConsumerMessage ... Message for a consumer
type ConsumerMessage struct {
	Message
	Id int64
}

// MessageRecipt ... Recipt for handling message
type MessageRecipt struct {
	Id      int64
	Success bool
}

// Pgmq ... Structure for holding message
type Pgmq struct {
	DB     *sql.DB
	Prefix string
}

func NewPgmq(db *sql.DB, prefix string) *Pgmq {
	return &Pgmq{DB: db, Prefix: prefix}
}

// CreateSchema ... builds any required tables
func (p *Pgmq) CreateSchema() error {
	s := fmt.Sprintf(createSchema, p.Prefix, p.Prefix, p.Prefix, p.Prefix, p.Prefix)
	_, err := p.DB.Exec(s)
	return err
}

// DropSchema ... removes any tables
func (p *Pgmq) DropSchema() error {
	s := fmt.Sprintf(dropScrema, p.Prefix, p.Prefix)
	_, err := p.DB.Exec(s)
	return err
}

// Publish ... This pushes a list of messages into the DB
func (p *Pgmq) Publish(messages []*Message) error {

	txn, err := p.DB.Begin()
	defer txn.Commit()
	if err != nil {
		return err
	}

	stmt, err := txn.Prepare(pq.CopyIn(fmt.Sprintf("%sq", p.Prefix), "topic", "payload"))
	if err != nil {
		return err
	}
	for _, m := range messages {
		_, err := stmt.Exec(m.Topic, m.Payload)
		if err != nil {
			return err
		}
	}
	_, err = stmt.Exec()
	return err
}

// Consumer ... This consumes a number of messages up to the limit
func (p *Pgmq) Consumer(topic string, size int, recipts chan *MessageRecipt) ([]*ConsumerMessage, error) {
	messages := make([]*ConsumerMessage, 0)
	q := fmt.Sprintf("SELECT id, topic, payload FROM %sq WHERE topic = $1 ORDER BY timestamp ASC FOR UPDATE SKIP LOCKED LIMIT $2;", p.Prefix)
	txn, err := p.DB.Begin()
	if err != nil {
		return messages, err
	}
	defer txn.Commit()

	stmt, err := p.DB.Prepare(q)
	if err != nil {
		return messages, err
	}
	defer stmt.Close()
	deleteQuery := fmt.Sprintf("DELETE FROM %sq WHERE id = ANY($1)", p.Prefix)
	deleteStmt, err := p.DB.Prepare(deleteQuery)
	if err != nil {
		return messages, err
	}
	defer deleteStmt.Close()

	rows, err := stmt.Query(topic, size)
	if err != nil {
		return messages, err
	}

	go func() {
		deleteIds := make([]int64, 0)
		for r := range recipts {
			if r.Success {
				deleteIds = append(deleteIds, r.Id)
			}
		}
		deleteStmt.Exec(pq.Array(deleteIds))
	}()
	defer rows.Close()
	for rows.Next() {
		var id int64
		var payload []byte
		var topic string
		rows.Scan(&id, &topic, &payload)
		messages = append(messages, &ConsumerMessage{Message: Message{Topic: topic, Payload: payload}, Id: id})
	}
	return messages, nil
}
