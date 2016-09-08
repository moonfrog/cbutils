//delete items older than n minutes, default 10 minutes

package main

import (
	"database/sql"
	"flag"
	_ "github.com/couchbase/go_n1ql"
	"os"
	"runtime"
	"sync"
	"time"

	log "github.com/moonfrog/badger/logger"
)

var serverURL = flag.String("server", "http://localhost:8093",
	"couchbase server URL")
var diff = flag.Int("diff", 10, "time in minutes")
var bucket = flag.String("bucket", "stats", "bucket to operate on")

var wg sync.WaitGroup

const (
	statsQuery   = "delete from stats where type is not null and timestamp > ? and timestamp < ?"
	economyQuery = "delete from economy where timestamp > ? and timestamp < ? and game_id = 3"
)

func main() {

	flag.Parse()
	log.SetLogFile("deleteItems")

	runtime.GOMAXPROCS(2)
	doneChan := make(chan bool)

	if *bucket == "stats" || *bucket == "all" {
		go runQuery(statsQuery, doneChan)
		wg.Add(1)
	}

	if *bucket == "economy" || *bucket == "all" {
		go runQuery(economyQuery, doneChan)
		wg.Add(1)
	}

	wg.Wait()
}

// run in a loop. Wake up every minute and delete a minute's
// worth of diff minutes old data

func runQuery(query string, doneChan chan bool) {
	defer wg.Done()

	log.Info("Starting query thread")
	ok := true
	for ok == true {

		select {
		case <-time.After(time.Minute * 1):
			go executeQuery(query)
		case <-doneChan:
			return
		}
	}

}

func executeQuery(query string) {

	n1ql, err := sql.Open("n1ql", *serverURL)
	if err != nil {
		log.Fatal(err)
	}

	err = n1ql.Ping()
	if err != nil {
		log.Fatal(err)
	}

	// Set query parameters
	os.Setenv("n1ql_timeout", "1000s")
	ac := []byte(`[{"user": "admin:Administrator", "pass": "asdasd"}]`)
	os.Setenv("n1ql_creds", string(ac))

	// differnce between start time and end time is 1 minute
	endTime := time.Now().Unix() - int64((*diff)*60)
	startTime := endTime - 60

	log.Info("Executing query %v %v %v", query, startTime, endTime)

	result, err := n1ql.Exec(query, startTime, endTime)

	queryTime := time.Now().Unix() - (endTime + (int64(*diff) * 60))

	if err != nil {
		log.Error(" Failed to execute query. Error %v", err)
	} else {

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			log.Fatal(err)
		}
		log.Info("Rows Deleted %d queryTime %v", rowsAffected, queryTime)
	}

}
