package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/couchbase/go_n1ql"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"
)

var serverURL = flag.String("server", "http://localhost:8093",
	"couchbase server URL")
var threads = flag.Int("threads", 1, "number of threads")
var queryFile = flag.String("queryfile", "query_file.txt", "file containing list of select queries")
var diff = flag.Int("diff", 600, "time difference")
var queryType = flag.String("type", "debug", "Query type debug or player")

func main() {

	flag.Parse()

	// set GO_MAXPROCS to the number of threads
	runtime.GOMAXPROCS(*threads)

	queryLines, err := readLines(*queryFile)
	if err != nil {
		log.Fatal(" Unable to read from file %s, Error %v", *queryFile, err)
	}

	for _, query := range queryLines {
		runQuery(*serverURL, query)
	}
}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func runQuery(server string, query string) {

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

	results := make([]interface{}, 0)
	percentileResults := make(map[string]interface{})

	var rows *sql.Rows

	startTime := time.Now()
	log.Printf(" running query %v %v", query, startTime.Unix()-int64(*diff))
	rows, err = n1ql.Query(query, startTime.Unix()-int64(*diff))
	if err != nil {
		log.Fatal("Error Query Line ", err, query)
	}

	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		log.Printf("No columns returned %v", err)
		return
	}
	if cols == nil {
		log.Printf("No columns returned")
		return
	}

	vals := make([]interface{}, len(cols))
	for i := 0; i < len(cols); i++ {
		vals[i] = new(interface{})
	}

	for rows.Next() {
		row := make(map[string]interface{})
		err = rows.Scan(vals...)
		if err != nil {
			fmt.Println(err)
			continue
		}
		for i := 0; i < len(vals); i++ {
			row[cols[i]] = returnValue(vals[i].(*interface{}))
		}
		results = append(results, row)

	}
	if rows.Err() != nil {
		log.Printf("Error sanning rows %v", err)
	}

	if *queryType == "debug" {
		percentileResults = handleDebugQuery(results)
	} else {
		percentileResults = handlePlayerQuery(results)
	}

	resultStr, _ := json.MarshalIndent(percentileResults, "", "    ")
	fmt.Printf("Query %v \n Result %s \n", query, resultStr)

}

func percentileN(numbers []float64, l, n int) float64 {
	i := l*n/100 - 1

	return numbers[i]
}

// calculate 50, 80 and 90th percentile
func handleDebugQuery(results []interface{}) map[string]interface{} {
	currentSubType := 1
	numbers := make([]float64, 0)
	var metric string
	var currentMetric string

	metricMap := make(map[string]interface{})
	percentileResults := make(map[string]interface{})

	for _, row := range results {
		/*
			log.Printf(" Row %v %v %v", row.(map[string]interface{})["subtype"], row.(map[string]interface{})["metric"],
				row.(map[string]interface{})["timeMsec"])
		*/

		metric = row.(map[string]interface{})["metric"].(string)
		if currentMetric == "" {
			metricMap[metric] = percentileResults
			currentMetric = metric
		} else if currentMetric != metric {
			// roll over
			percentileResults = make(map[string]interface{})
			metricMap[metric] = percentileResults
			currentMetric = metric
		}

		st, _ := strconv.ParseInt(row.(map[string]interface{})["subtype"].(string), 10, 64)
		subType := int(st)
		if subType != currentSubType {
			// roll-over the subType and calculate the percentile for the
			// current set of numbers
			subTypeMap := make(map[string]interface{})
			subTypeMap["50th percentile"] = percentileN(numbers, len(numbers), 50)
			subTypeMap["80th percentile"] = percentileN(numbers, len(numbers), 80)
			subTypeMap["90th percentile"] = percentileN(numbers, len(numbers), 90)
			percentileResults[string(currentSubType)] = subTypeMap

			numbers = make([]float64, 0)
			currentSubType = subType

		} else {
			timeMsec, _ := strconv.ParseFloat(row.(map[string]interface{})["timeMsec"].(string), 64)
			numbers = append(numbers, timeMsec)
		}
	}

	if len(numbers) > 0 {
		subTypeMap := make(map[string]interface{})
		subTypeMap["50th percentile"] = percentileN(numbers, len(numbers), 50)
		subTypeMap["80th percentile"] = percentileN(numbers, len(numbers), 80)
		subTypeMap["90th percentile"] = percentileN(numbers, len(numbers), 90)
		percentileResults[string(currentSubType)] = subTypeMap

	}

	return metricMap
}

// calculate 50, 80 and 90th percentile
func handlePlayerQuery(results []interface{}) map[string]interface{} {
	numbers := make([]float64, 0)
	percentileResults := make(map[string]interface{})
	for _, row := range results {
		var inner interface{}
		json.Unmarshal([]byte(row.(map[string]interface{})["duration"].(string)), &inner)
		//log.Printf(" inner %v", inner)
		numbers = append(numbers, inner.(map[string]interface{})["duration"].(float64))
	}

	percentileResults["50th percentile"] = percentileN(numbers, len(numbers), 50)
	percentileResults["80th percentile"] = percentileN(numbers, len(numbers), 80)
	percentileResults["90th percentile"] = percentileN(numbers, len(numbers), 90)

	return percentileResults
}

func returnValue(pval *interface{}) interface{} {
	switch v := (*pval).(type) {
	case nil:
		return "NULL"
	case bool:
		if v {
			return true
		} else {
			return false
		}
	case []byte:
		return string(v)
	case time.Time:
		return v.Format("2006-01-02 15:04:05.999")
	default:
		return v
	}
}
