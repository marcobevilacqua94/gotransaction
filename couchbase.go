package main

import (
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	gocb "github.com/couchbase/gocb/v2"
)

func main() {

	timeout, _ := strconv.Atoi(os.Args[6])
	// #tag::connect[]
	opts := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: os.Args[2],
			Password: os.Args[3],
		},
		TransactionsConfig: gocb.TransactionsConfig{
			DurabilityLevel: gocb.DurabilityLevelNone,
			Timeout:         time.Duration(timeout) * time.Second,
		},
	}
	cluster, err := gocb.Connect(os.Args[1], opts)
	if err != nil {
		panic(err)
	}

	bucket := cluster.Bucket("test")
	collection := bucket.Scope("test").Collection("test")

	// We wait until the bucket is connected and setup.
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		panic(err)
	}
	// #end::connect[]

	// #tag::workers[]
	type args struct {
		Name  string
		Data  interface{}
		index int
	}
	concurrency := MaxParallelism() * 24 // number of goroutines
	workChan := make(chan args, concurrency)
	shutdownChan := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	wg.Add(concurrency)
	size, _ := strconv.Atoi(os.Args[5])
	body := randomDigits(size)
	start := time.Now()
	_, err1 := cluster.Transactions().Run(func(ctx *gocb.TransactionAttemptContext) error {
		_, err := ctx.Insert(collection, "0", map[string]interface{}{"body": body})
		if err != nil {
			return err
		}
		for i := 0; i < concurrency; i++ {
			go func() {
				for {
					select {
					case args := <-workChan:
						_, err := ctx.Insert(collection, args.Name, args.Data)
						if args.index%1000 == 0 {
							log.Println(args.index)
						}
						if err != nil {
							log.Println(err)
						}
					case <-shutdownChan:
						wg.Done()
						return
					}
				}
			}()
		}

		total, _ := strconv.Atoi(os.Args[4])
		for i := 1; i < total; i++ {
			workChan <- args{
				Name:  fmt.Sprint(i),
				Data:  map[string]interface{}{"body": body},
				index: i,
			}
		}

		return nil
	}, nil)
	var ambigErr gocb.TransactionCommitAmbiguousError
	if errors.As(err1, &ambigErr) {
		log.Println("Transaction possibly committed")

		log.Printf("%+v", ambigErr)
		return
	}
	var failedErr gocb.TransactionFailedError
	if errors.As(err1, &failedErr) {
		log.Println("Transaction did not reach commit point")

		log.Printf("%+v", failedErr)
		return
	}
	if err1 != nil {
		panic(err1)
	}

	for i := 0; i < concurrency; i++ {
		shutdownChan <- struct{}{}
	}
	wg.Wait()
	cluster.Close(nil)
	log.Println("Completed")
	elapsed := time.Since(start)
	log.Printf("%s", elapsed)
}

func randomInt(n int) int {
	num, _ := rand.Int(rand.Reader, big.NewInt(int64(n)))
	return int(num.Int64())
}

func randomDigits(length int) string {
	var digits strings.Builder
	for i := 0; i < length; i++ {
		digit := randomInt(10)
		digits.WriteString(fmt.Sprint(digit))
	}
	return digits.String()
}

func MaxParallelism() int {
	maxProcs := runtime.GOMAXPROCS(0)
	numCPU := runtime.NumCPU()
	if maxProcs < numCPU {
		return maxProcs
	}
	return numCPU
}
