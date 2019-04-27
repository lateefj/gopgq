package liteq

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lateefj/gq"

	// sqlite3 database package impoart
	_ "github.com/mattn/go-sqlite3"
)

// TimeWithMsSqlite ... Special constant to get a time with milliseconds. This is helpful for checkout as the timeout might be sub second
const TimeWithMsSqlite = "STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')"

var (
	createSchema = `
CREATE TABLE IF NOT EXISTS %sq (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	checkout TIMESTAMP,
	payload BLOB
);
CREATE INDEX IF NOT EXISTS %sq_timestamp_idx ON %sq (checkout ASC, timestamp ASC);
`
	dropScrema = `
DROP TABLE IF EXISTS %sq;
`
)

// Liteq Structure for sqlite
type Liteq struct {
	DB     *sql.DB
	Prefix string
	TTL    time.Duration
	exit   bool
	mutex  *sync.RWMutex
}

// Create ... builds any required tables
func (l *Liteq) Create() error {
	if l.mutex == nil {
		l.mutex = &sync.RWMutex{}
	}
	s := fmt.Sprintf(createSchema, l.Prefix, l.Prefix, l.Prefix)
	_, err := l.DB.Exec(s)
	return err
}

// Destroy ... removes any tables
func (l *Liteq) Destroy() error {
	s := fmt.Sprintf(dropScrema, l.Prefix)
	_, err := l.DB.Exec(s)
	return err
}

// StopConsumer ... Stop consuming messages
func (l *Liteq) StopConsumer() {
	l.mutex.Lock()
	l.exit = true
	l.mutex.Unlock()
}

// Exit ...
func (l *Liteq) Exit() bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.exit
}

// Publish ... This pushes a list of messages into the DB
func (l *Liteq) Publish(messages []*gq.Message) error {

	txn, err := l.DB.Begin()
	defer txn.Commit()
	if err != nil {
		return err
	}

	q := fmt.Sprintf("INSERT INTO %sq (payload) VALUES(?);", l.Prefix)
	stmt, err := txn.Prepare(q)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, m := range messages {
		_, err := stmt.Exec(m.Payload)
		if err != nil {
			return err
		}
	}
	return err
}

// Commit ... Removes any messages that bave been comsusumed by the b
func (l *Liteq) Commit(recipts []*gq.Receipt) error {
	deleteQuery := fmt.Sprintf("DELETE FROM %sq WHERE id IN(?)", l.Prefix)
	deleteStmt, err := l.DB.Prepare(deleteQuery)
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
	deleteClause := strings.Trim(strings.Replace(fmt.Sprint(deleteIds), " ", ",", -1), "[]")
	_, err = deleteStmt.Exec(deleteClause)
	return err
}

// ConsumeBatch ... This consumes a number of messages up to the limit
func (l *Liteq) ConsumeBatch(size int) ([]*gq.ConsumerMessage, error) {
	ms := make([]*gq.ConsumerMessage, 0)
	// Find
	q := fmt.Sprintf("SELECT id, payload FROM %sq WHERE checkout IS null", l.Prefix)
	// If there is a TTL then checkout messages that have expired
	if l.TTL.Seconds() > 0.0 {
		q = fmt.Sprintf("%s OR DATETIME(checkout,  '%f second') < %s", q, l.TTL.Seconds(), TimeWithMsSqlite)
	}
	// Order and limit
	q = fmt.Sprintf("%s ORDER BY checkout ASC, timestamp ASC LIMIT $1;", q)
	txn, err := l.DB.Begin()
	if err != nil {
		return ms, err
	}
	defer txn.Commit()

	stmt, err := l.DB.Prepare(q)
	if err != nil {
		return ms, err
	}
	defer stmt.Close()

	var rows *sql.Rows

	rows, err = stmt.Query(size)
	if err != nil {
		return ms, err
	}

	checkoutIds := make([]int64, 0)
	defer rows.Close()
	for rows.Next() {
		var id int64
		var payload []byte
		rows.Scan(&id, &payload)
		checkoutIds = append(checkoutIds, id)
		ms = append(ms, &gq.ConsumerMessage{Message: gq.Message{Payload: payload}, Id: id})
	}

	// Query any messages that have not been checked out
	uq := fmt.Sprintf("UPDATE %sq SET checkout = %s WHERE id IN (?);", l.Prefix, TimeWithMsSqlite)

	update, err := l.DB.Prepare(uq)
	if err != nil {
		return ms, err
	}
	checkoutClause := strings.Trim(strings.Replace(fmt.Sprint(checkoutIds), " ", ",", -1), "[]")
	_, err = update.Exec(checkoutClause)
	if err != nil {
		txn.Rollback()
		return nil, err
	}
	defer stmt.Close()
	return ms, nil
}

// Stream ... Creates a stream of consumption
func (l *Liteq) Stream(size int, messages chan []*gq.ConsumerMessage, pause time.Duration) {
	defer close(messages)
	for {

		// Consume until there are no more messages or there is an error
		// No messages there was an error or time to exit
		for {
			if l.Exit() {
				return
			}
			ms, err := l.ConsumeBatch(size)
			// If exit then
			if len(ms) == 0 || err != nil {
				break
			}
			messages <- ms
		}
		// Breather so not just infinate loop of queries
		time.Sleep(pause)
	}
}
