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
	"time"

	"github.com/lateefj/gopgq/mq"
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
	db           *sql.DB
	outPath      string
	inPath       string
	outFile      *os.File
	inFile       *os.File
)

func init() {
	user := os.Getenv("USER")
	defaultDsn := fmt.Sprintf("postgres://%s:@localhost/pgmq?sslmode=disable", user)
	flag.IntVar(&producerSize, "prod", runtime.NumCPU(), "producers")
	flag.IntVar(&consumerSize, "cons", runtime.NumCPU(), "consumers")
	flag.StringVar(&dsn, "dsn", defaultDsn, fmt.Sprintf("database connection info example %s", defaultDsn))
	flag.IntVar(&maxConn, "conn", 64, "max database connections")
	flag.IntVar(&maxNumber, "maxnumb", 1000, "max number of messages to batch")
	flag.IntVar(&minNumber, "minnumb", 1, "min number of messages to batch")
	flag.IntVar(&mulitplier, "multiplier", 2, "multiplier")
	flag.StringVar(&outPath, "out", "", "file to output default to stdout")
	flag.StringVar(&inPath, "in", "", "file to input default to stdin")
}

func makeProducers(wg sync.WaitGroup, size, messageSize int, comments chan [][]byte) {
	for i := 0; i < size; i++ {
		go func() {
			wg.Add(1)
			q := mq.NewPgmq(db, topic)
			for ms := range comments {
				tmp := make([]*mq.Message, messageSize)
				for j, m := range ms {
					tmp[j] = &mq.Message{Topic: topic, Payload: m}
				}
				q.Publish(tmp)
			}
			wg.Done()
		}()
	}
}

func makeConsumers(wg sync.WaitGroup, size, messageSize int) {
	for i := 0; i < size; i++ {
		go func() {
			wg.Add(1)
			for {
				q := mq.NewPgmq(db, topic)
				recipts := make(chan *mq.MessageRecipt)
				consumedMessages, err := q.Consumer(topic, messageSize, recipts)
				if err != nil {
					fmt.Printf("Failed to consumer %s\n", err)
				}
				if len(consumedMessages) == 0 {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				count := 0
				for _, m := range consumedMessages {
					count += 1
					recipts <- &mq.MessageRecipt{Id: m.Id, Success: true}
				}
				close(recipts)
			}
			wg.Done()
		}()
	}
}

func main() {
	// Close the output file
	flag.Parse()
	if outPath != "" {
		outFile, _ = os.OpenFile(outPath, os.O_APPEND|os.O_WRONLY, 0600)
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
	writer.Write([]string{"total_messages", "elapsed_seconds", "messages_per_second"})
	writer.Flush()
	for messageSize := minNumber; messageSize <= maxNumber; messageSize = messageSize * mulitplier {
		go func() {
			var err error
			db, err = sql.Open("postgres", dsn)
			if err != nil {
				log.Fatal(err)
				return
			}
			defer db.Close()
			db.SetMaxOpenConns(maxConn)

			// Initialize the database stuff
			q := mq.NewPgmq(db, topic)
			q.CreateSchema()

			// Destroy when done
			defer q.DropSchema()
			inFile.Seek(int64(0), 0)
			//fmt.Printf("messageSize %d maxNumber %d\n", messageSize, maxNumber)
			start := time.Now()
			// Create a buffer for the reddit comments that can buffer at least the number of consumers
			commentBuffer := make(chan [][]byte, producerSize)
			var wg sync.WaitGroup
			makeProducers(wg, producerSize, messageSize, commentBuffer)
			makeConsumers(wg, consumerSize, messageSize)
			comments := make([][]byte, messageSize)
			scanner := bufio.NewScanner(inFile)

			runningTotal := 0
			counter := 0
			for scanner.Scan() {
				comments[counter] = []byte(scanner.Text())
				counter += 1
				runningTotal += 1
				if counter == messageSize {
					commentBuffer <- comments
					counter = 0
					//fmt.Printf("Running total: %d\r", runningTotal)
				}
			}
			close(commentBuffer)
			wg.Wait()

			diff := time.Now().Sub(start)
			total := fmt.Sprintf("%d", runningTotal)
			runtimeSeconds := fmt.Sprintf("%f", diff.Seconds())
			messagesPerSecond := fmt.Sprintf("%f", float64(runningTotal)/diff.Seconds())
			fmt.Printf("Total comments: %s Elapsed Seconds: %s comments per second %s \n", total, runtimeSeconds, messagesPerSecond)
			writer.Write([]string{total, runtimeSeconds, messagesPerSecond})
			writer.Flush()
		}()
	}
}
