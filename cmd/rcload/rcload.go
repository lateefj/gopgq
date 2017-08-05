package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lateefj/gq"
	"github.com/lateefj/gq/pgmq"
)

const (
	topic = "rcload_"
)

var (
	producerSize int
	consumerSize int
	dsn          string
	maxConn      int
	minNumber    int
	maxNumber    int
	mulitplier   int
	outPath      string
	inPath       string
	outFile      *os.File
	inFile       *os.File
	status       *Status
)

type Status struct {
	totalProduced     int32
	totalConsumed     int32
	activelyProducing bool
	activelyConsuming bool
	mutex             *sync.RWMutex
}

func NewStatus() *Status {
	return &Status{totalProduced: 0, totalConsumed: 0, activelyProducing: true, activelyConsuming: true, mutex: &sync.RWMutex{}}
}
func (s *Status) producedCount() int32 {
	return atomic.LoadInt32(&s.totalProduced)
}

func (s *Status) incProduced() int32 {
	return atomic.AddInt32(&s.totalProduced, 1)
}

func (s *Status) consumedCount() int32 {
	return atomic.LoadInt32(&s.totalConsumed)
}

func (s *Status) incConsumed() int32 {
	return atomic.AddInt32(&s.totalConsumed, 1)
}

func (s *Status) producing() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.activelyProducing

}
func (s *Status) finishedProducing() {
	s.mutex.Lock()
	s.activelyProducing = false
	s.mutex.Unlock()
}

func (s *Status) consuming() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.activelyConsuming
}

func (s *Status) finishedConsuming() {
	s.mutex.Lock()
	s.activelyConsuming = false
	s.mutex.Unlock()
}

func init() {
	user := os.Getenv("USER")
	defaultDsn := fmt.Sprintf("postgres://%s:@localhost/pgmq?sslmode=disable", user)
	flag.IntVar(&producerSize, "prod", runtime.NumCPU(), "producers")
	flag.IntVar(&consumerSize, "cons", runtime.NumCPU(), "consumers")
	flag.StringVar(&dsn, "dsn", defaultDsn, fmt.Sprintf("database connection info example %s", defaultDsn))
	flag.IntVar(&maxConn, "conn", 100, "max database connections")
	flag.IntVar(&maxNumber, "maxnumb", 1000, "max number of messages to batch")
	flag.IntVar(&minNumber, "minnumb", 1, "min number of messages to batch")
	flag.IntVar(&mulitplier, "multiplier", 2, "multiplier")
	flag.StringVar(&outPath, "out", "", "file to output default to stdout")
	flag.StringVar(&inPath, "in", "", "file to input default to stdin")
}
func db() (*sql.DB, error) {
	return sql.Open("postgres", dsn)
}
func makeProducers(wg *sync.WaitGroup, size, messageSize int, comments chan [][]byte) {
	for i := 0; i < size; i++ {
		wg.Add(1)
		go func(producerNumber int) {
			defer wg.Done()
			db, err := db()
			if err != nil {
				log.Printf("Failed to make database connection %s\n", err)
				return
			}
			defer db.Close()
			q := pgmq.NewPgmq(db, topic)
			for {
				ms, more := <-comments
				if !more {
					//fmt.Printf("Producer %d Total Produced %d\n", producerNumber, atomic.LoadInt32(&totalProduced))
					return
				}
				tmp := make([]*gq.Message, messageSize)
				for j, m := range ms {
					tmp[j] = &gq.Message{Payload: m}
				}
				err := q.Publish(tmp)
				// Only increment the counter if the publish was successful
				if err == nil {
					for _ = range ms {
						status.incProduced()
					}
				}
			}
		}(i)
	}
}

func makeConsumers(wg *sync.WaitGroup, size, messageSize int) {
	for consumerId := 0; consumerId < size; consumerId++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			db, err := db()
			if err != nil {
				log.Printf("Failed to make database connection %s\n", err)
				return
			}
			defer db.Close()
			q := pgmq.NewPgmq(db, topic)

			stream := make(chan []*gq.ConsumerMessage, messageSize)
			go q.Stream(messageSize, stream, 10*time.Millisecond)
			for {
				select {
				case consumedMessages, more := <-stream:
					if !more {
						//fmt.Printf("Total consumer %d consumed %d\n", id, status.consumedCount())
						return
					}
					//fmt.Printf("Total consumed %d\r", status.consumedCount())
					receipts := make([]*gq.Receipt, len(consumedMessages))
					for i, m := range consumedMessages {
						receipts[i] = &gq.Receipt{Id: m.Id, Success: true}
						status.incConsumed()
					}
					q.Commit(receipts)
				case <-time.NewTimer(100 * time.Millisecond).C:
				}

				// If we have consumed all the messages then exit
				if !status.producing() && status.consumedCount() >= status.producedCount() {
					q.StopConsumer()
				}
			}
		}(consumerId)
	}
}

func main() {
	// Close the output file
	flag.Parse()
	var err error
	if outPath != "" {
		if _, err := os.Stat(outPath); os.IsNotExist(err) {
			outFile, err = os.OpenFile(outPath, os.O_APPEND|os.O_WRONLY, 0655)
		} else {
			outFile, err = os.Open(outPath)
		}
		if err != nil {
			log.Fatalf("Failed to open output file %s error %s", outPath, err)
		}
		defer outFile.Close()
	} else {
		fmt.Printf("Using standard out to write file \n")
		outFile = os.Stdout
	}
	if inPath != "" {
		inFile, _ = os.Open(inPath)
	} else {
		inFile = os.Stdin
	}
	fmt.Printf("Producers %d consumers %d message min number %d max number %d mulitplier %d \n", producerSize, consumerSize, minNumber, maxNumber, mulitplier)

	writer := csv.NewWriter(outFile)
	writer.Write([]string{"type", "total_messages", "elapsed_seconds", "messages_per_second"})
	for messageSize := minNumber; messageSize <= maxNumber; messageSize = messageSize * mulitplier {
		status = NewStatus()
		func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered in f", r)
				}
			}()
			db, err := db()
			if err != nil {
				log.Printf("DB FAILURE run %d error %s\n", messageSize, err)
				return
			}
			defer db.Close()
			db.SetMaxOpenConns(maxConn)

			// Initialize the database stuff
			q := pgmq.NewPgmq(db, topic)
			q.DropSchema()
			q.CreateSchema()

			// Destroy when done
			defer q.DropSchema()
			// Get consumers started
			var consumerWg sync.WaitGroup
			consumerStart := time.Now()
			makeConsumers(&consumerWg, consumerSize, messageSize)
			inFile.Seek(int64(0), 0)
			//fmt.Printf("messageSize %d maxNumber %d\n", messageSize, maxNumber)
			start := time.Now()
			totalMessages := int32(0)
			// Create a buffer for the reddit comments that can buffer at least the number of consumers
			commentBuffer := make(chan [][]byte, producerSize)
			var producerWg sync.WaitGroup
			makeProducers(&producerWg, producerSize, messageSize, commentBuffer)
			// Buffer for comments
			comments := make([][]byte, messageSize)
			scanner := bufio.NewScanner(inFile)
			// Get one line at a time
			scanner.Split(bufio.ScanLines)
			counter := 0
			for scanner.Scan() {
				// Get a line
				comments[counter] = []byte(scanner.Text())
				counter += 1
				atomic.AddInt32(&totalMessages, 1)
				if counter == messageSize {
					commentBuffer <- comments
					counter = 0
				}
			}
			close(commentBuffer)
			//fmt.Printf("Waiting on producers \n")
			producerWg.Wait()
			status.finishedProducing()
			diff := time.Now().Sub(start)
			total := fmt.Sprintf("%d", totalMessages)
			runtimeSeconds := fmt.Sprintf("%f", diff.Seconds())
			messagesPerSecond := fmt.Sprintf("%f", float64(totalMessages)/diff.Seconds())
			fmt.Printf("Batch size %d producered %d Total comments: %s Elapsed Seconds: %s produced %s comments per second \n", messageSize, status.producedCount(), total, runtimeSeconds, messagesPerSecond)
			err = writer.Write([]string{"producer", total, runtimeSeconds, messagesPerSecond})
			if err != nil {
				log.Printf("csv writer failure %s\n", err)
			}
			// Wait for the consumers to finish
			//fmt.Printf("Waiting on consumers \n")
			consumerWg.Wait()
			status.finishedConsuming()

			diff = time.Now().Sub(consumerStart)
			runtimeSeconds = fmt.Sprintf("%f", diff.Seconds())
			messagesPerSecond = fmt.Sprintf("%f", float64(totalMessages)/diff.Seconds())
			fmt.Printf("Batch size %d Consumers comments: %s Elapsed Seconds: %s consumed %s comments per second \n", messageSize, total, runtimeSeconds, messagesPerSecond)
			diff = time.Now().Sub(start)
			runtimeSeconds = fmt.Sprintf("%f", diff.Seconds())
			messagesPerSecond = fmt.Sprintf("%f", float64(totalMessages)/diff.Seconds())
			fmt.Printf("Batch size %d Total processed comments: %s Elapsed Seconds: %s at %s comments per second \n", messageSize, total, runtimeSeconds, messagesPerSecond)
			err = writer.Write([]string{"consumer", total, runtimeSeconds, messagesPerSecond})
			if err != nil {
				log.Printf("csv writer failure %s\n", err)
			}
			time.Sleep(10 * time.Second)
		}()
	}
	outFile.Close()
}
