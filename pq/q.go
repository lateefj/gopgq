package pq

import (
	"bytes"
	"database/sql"
	"fmt"
	"sync"
	"text/template"
	"time"

	"github.com/lateefj/gq"
	pq "github.com/lib/pq" // Postgresql Driver
)

// TODO: Need a way to find the optimal vacuum threshold / analyze thresholds based on
// message payload
var createSchema = `
CREATE SEQUENCE IF NOT EXISTS {{.TableName}}q_id_seq;
CREATE TABLE IF NOT EXISTS {{.TableName}}q (
	id INT8 NOT NULL DEFAULT nextval('{{.TableName}}q_id_seq') PRIMARY KEY,
	timestamp TIMESTAMP NOT NULL DEFAULt now(),
	checkout TIMESTAMP,
	payload BYTEA
);
CREATE INDEX IF NOT EXISTS {{.TableName}}q_timestamp_idx ON {{.TableName}}q (checkout ASC NULLS FIRST, timestamp ASC);
ALTER TABLE {{.TableName}}q SET (autovacuum_vacuum_scale_factor = 0.0);
ALTER TABLE {{.TableName}}q SET (autovacuum_vacuum_threshold = 250000);
ALTER TABLE {{.TableName}}q SET (autovacuum_analyze_scale_factor = 0.0);
ALTER TABLE {{.TableName}}q SET (autovacuum_analyze_threshold = 50000);
`
var dropScrema = `
DROP TABLE IF EXISTS {{.TableName}}q;
DROP SEQUENCE IF EXISTS {{.TableName}}q_id_seq;
`

// Pgmq ... Structure for holding message
type Pgmq struct {
	DB     *sql.DB
	Prefix string
	Ttl    time.Duration
	exit   bool
	Mutex  *sync.RWMutex
}

func NewPgmq(db *sql.DB, prefix string) *Pgmq {
	return &Pgmq{DB: db, Prefix: prefix, Ttl: 0 * time.Millisecond, exit: false, Mutex: &sync.RWMutex{}}
}

// Create... builds any required tables
func (p *Pgmq) Create() error {
	d := struct{ TableName string }{
		TableName: p.Prefix,
	}

	t := template.Must(template.New("create_table").Parse(createSchema))
	var b bytes.Buffer
	err := t.Execute(&b, d)
	if err != nil {
		return err
	}
	_, err = p.DB.Exec(b.String())
	return err
}

// Destroy ... removes any tables
func (p *Pgmq) Destroy() error {
	d := struct{ TableName string }{
		TableName: p.Prefix,
	}

	t := template.Must(template.New("drop_table").Parse(dropScrema))
	var b bytes.Buffer
	err := t.Execute(&b, d)
	if err != nil {
		return err
	}
	_, err = p.DB.Exec(b.String())
	return err
}

func (p *Pgmq) StopConsumer() {
	p.Mutex.Lock()
	p.exit = true
	p.Mutex.Unlock()
}

func (p *Pgmq) Exit() bool {
	p.Mutex.RLock()
	defer p.Mutex.RUnlock()
	return p.exit

}

// Publish ... This pushes a list of messages into the DB
func (p *Pgmq) Publish(messages []*gq.Message) error {

	txn, err := p.DB.Begin()
	defer txn.Commit()
	if err != nil {
		fmt.Printf("Error with %s\n", err)
		return err
	}

	stmt, err := txn.Prepare(pq.CopyIn(fmt.Sprintf("%sq", p.Prefix), "payload"))
	if err != nil {
		return err
	}
	for _, m := range messages {
		_, err := stmt.Exec(m.Payload)
		if err != nil {
			return err
		}
	}
	_, err = stmt.Exec()
	return err
}

func (p *Pgmq) Commit(recipts []*gq.Receipt) error {
	deleteQuery := fmt.Sprintf("DELETE FROM %sq WHERE id = ANY($1)", p.Prefix)
	deleteStmt, err := p.DB.Prepare(deleteQuery)
	if err != nil {
		return err
	}
	defer deleteStmt.Close()
	deleteIds := make([]int64, 0)
	for _, r := range recipts {
		if r.Success {
			deleteIds = append(deleteIds, r.Id)
		}
	}
	_, err = deleteStmt.Exec(pq.Array(deleteIds))
	return err
}

// ConsumeBatch ... This consumes a number of messages up to the limit
func (p *Pgmq) ConsumeBatch(size int) ([]*gq.ConsumerMessage, error) {
	ms := make([]*gq.ConsumerMessage, 0)
	// Query any messages that have not been checked out
	q := fmt.Sprintf("UPDATE %sq SET checkout = now() WHERE id IN (SELECT id FROM %sq WHERE checkout IS null ", p.Prefix, p.Prefix)
	// If there is a TTL then checkout messages that have expired
	if p.Ttl.Seconds() > 0.0 {
		q = fmt.Sprintf("OR checkout + $2 > now()")
	}
	q = fmt.Sprintf("%s ORDER BY checkout ASC NULLS FIRST, timestamp ASC FOR UPDATE SKIP LOCKED LIMIT $1) RETURNING id, payload;", q)
	//fmt.Printf("%s\n", q)
	txn, err := p.DB.Begin()
	if err != nil {
		return ms, err
	}
	defer txn.Commit()

	stmt, err := p.DB.Prepare(q)
	if err != nil {
		return ms, err
	}
	defer stmt.Close()

	var rows *sql.Rows

	// TTL queries takes an extra param
	if p.Ttl.Seconds() > 0.0 {
		rows, err = stmt.Query(size, p.Ttl)
	} else {
		rows, err = stmt.Query(size)
	}
	if err != nil {
		return ms, err
	}

	defer rows.Close()
	for rows.Next() {
		var id int64
		var payload []byte
		rows.Scan(&id, &payload)
		ms = append(ms, &gq.ConsumerMessage{Message: gq.Message{Payload: payload}, Id: id})
	}
	return ms, nil
}

// Stream ... Creates a stream of consumption
func (p *Pgmq) Stream(size int, messages chan []*gq.ConsumerMessage, pause time.Duration) {
	defer close(messages)
	for {

		// Consume until there are no more messages or there is an error
		// No messages there was an error or time to exit
		for {
			if p.Exit() {
				return
			}
			ms, err := p.ConsumeBatch(size)
			// If exit then
			if len(ms) == 0 || err != nil {
				break
			}
			messages <- ms
		}
		// Breather so not just infinite loop of queries
		time.Sleep(pause)
	}
}
