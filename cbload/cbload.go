package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/amz.v1/aws"
	"gopkg.in/amz.v1/s3"

	"github.com/couchbase/go-couchbase"
	"github.com/jeffail/tunny"
	"github.com/moonfrog/badger/common"
	log "github.com/moonfrog/badger/logger"
	"github.com/moonfrog/badger/zootils"
)

var fileMap, newMap map[string]bool

type CouchbaseConfig struct {
	ServerURL string
	Bucket    string
}

// global
var cbConfig CouchbaseConfig
var maxThreads int
var wg sync.WaitGroup
var downloadWg sync.WaitGroup

type S3Config struct {
	AwsKey    string
	AwsSecret string
}

var s3Bucket = flag.String("s3Bucket", "badger-dev-backups", "s3 bucket containing the log files")
var cbBucket = flag.String("cbBucket", "m_table_economy_cash", "couchbase bucket")
var tdiff = flag.Int("tdiff", 12, "time window")
var basedir = flag.String("baseDir", "/tmp", "base directory for saving files")
var scale = flag.Int("scale", 1, "scale factor")

var excludeCols = []string{"date", "day", "hour", "minute", "month", "second", "year", "time"}

func main() {

	flag.Parse()

	maxThreads = runtime.NumCPU() * *scale
	runtime.GOMAXPROCS(maxThreads)

	common.Init("cbload", 3000)
	err := zootils.GetInstance().LoadConfig(&cbConfig, "config/couchbase", func(string) {})

	if err != nil {
		log.Fatal("Couldn't load config. Err - %s", err)

	}

	if cbConfig.ServerURL == "" || cbConfig.Bucket == "" {
		log.Fatal("Config error %v", cbConfig)
	}

	var s3Config S3Config
	zootils.GetInstance().LoadConfig(&s3Config, "config/s3Config", func(string) {})
	if s3Config.AwsKey == "" || s3Config.AwsSecret == "" {
		log.Fatal("Missing aws credentials. AwsKey - %s, awsSecret - %s.", s3Config.AwsKey, s3Config.AwsSecret)
	}

	auth := aws.Auth{s3Config.AwsKey, s3Config.AwsSecret}
	s3b := s3.New(auth, aws.USEast).Bucket(*s3Bucket)

	list, err := s3b.List("stats@economy@cash@", "", "", 1000)
	data := []string{}
	if err == nil {
		for len(list.Contents) != 0 {
			data = populateList(data, list)
			marker := list.Contents[len(list.Contents)-1].Key
			list, err = s3b.List("stats@economy@cash@", "", marker, 1000)
			if err != nil {
				log.Warn("Could not stat s3 bucket, err: %v", err)
				break
			}
		}
	} else {
		log.Warn("Connection error: %v", err)
	}

	filtered := processList(data)
	if len(filtered) == 0 {
		log.Fatal("No files to process")
	}

	log.Info("Number of files to process %v", len(filtered))

	loaderChan := make(chan string, 20)
	doneChan := make(chan bool)
	go processFile(loaderChan, doneChan)

	fList := make([][]string, 2)

	for i, file := range filtered {
		fList[i%2] = append(fList[i%2], file)
	}

	for i := 0; i < 2; i++ {
		downloadWg.Add(1)
		go downloadFiles(fList[i], s3b, loaderChan)
	}

	// wait for the download tasks to complete before
	// closing the loaderChan
	downloadWg.Wait()
	close(loaderChan)
	<-doneChan

}

func populateList(data []string, list *s3.ListResp) []string {
	for _, elem := range list.Contents {
		if strings.Contains(elem.Key, "gz") {
			if fileMap[elem.Key] {
				log.Info("duplicate file upload attempted %v", elem.Key)
				newMap[elem.Key] = true
			} else {
				data = append(data, elem.Key)
			}
		}
	}
	return data
}

// filter those files whose timestamp lies within a certain window
func processList(fileList []string) []string {

	filtered := make([]string, 0)
	start := time.Now().Add(-time.Duration(*tdiff) * time.Hour).Unix()

	for _, file := range fileList {
		parts := strings.Split(file, "-")
		rawTS := strings.Split(parts[2], ".")[0]
		ts, err := strconv.ParseInt(rawTS, 10, 64)
		if err != nil {
			log.Error(" Unable to parse file . Parts %v. Error %v", rawTS, err)
		}

		if ts >= start {
			filtered = append(filtered, file)
		}
	}

	return filtered
}

// download files from s3 and queue files for loading into cb
func downloadFiles(fileList []string, bucket *s3.Bucket, loaderChan chan string) {
	defer downloadWg.Done()

	for _, file := range fileList {
	retry:
		fileBytes, err := bucket.Get(file)
		if err != nil {
			log.Error("Get failed %v", err)
			goto retry // retry endlessly
		}

		localFile := *basedir + "/" + file

		err = ioutil.WriteFile(localFile, fileBytes, 0644)
		if err != nil {
			log.Fatal("Writing to file failed %v", err)
		}

		loaderChan <- localFile
	}
}

type work struct {
	filePath string
	cbBucket *couchbase.Bucket
}

var numQueued int
var numProcessed int

func processFile(loaderChan chan string, doneChan chan bool) {

	defer close(doneChan)

	numFiles := 0
	client, err := couchbase.Connect(cbConfig.ServerURL)
	if err != nil {
		log.Fatal("Unable to connect to couchbase server %v", err)
	}

	pool, err := client.GetPool("default")
	if err != nil {
		log.Fatal("Default pool not found %v", err)
	}

	bucket, err := pool.GetBucket(*cbBucket)
	if err != nil {
		log.Fatal("Bucket %d not found", *cbBucket)
	}

	threadPool, _ := tunny.CreatePool(maxThreads, unzipAndLoad).Open()

	ok := true

	for ok {
		select {
		case fp, ok := <-loaderChan:
			if ok {
				//queue work to the threadpool
				wg.Add(1)
				go func() {
					work := &work{filePath: fp, cbBucket: bucket}
					err, _ := threadPool.SendWork(work)
					if err != nil {
						log.Error("Unzip and Load Returned error %v", err)
					}
					numFiles++
				}()

			}

			numQueued++
			log.Info("===== Queued %v", numQueued)

		}
	}

	log.Info("Finished queueing all jobs %v", numFiles)
	wg.Wait()

}

func unzipAndLoad(w interface{}) interface{} {

	defer wg.Done()

	filePath := w.(*work).filePath
	cbBucket := w.(*work).cbBucket

	file, err := os.Open(filePath)
	if err != nil {
		log.Error("Unable to open file for reading %v", err)
		return err
	}
	reader, err := gzip.NewReader(file)
	if err != nil {
		return err
	}

	var lines []string
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	file.Close()

	docs, err := jsonifyFile(lines)
	if err != nil {
		log.Error("Failed to jsonify file %v", err)
	}

	errs := loadKeys(cbBucket, docs)
	if errs != nil {
		log.Error("Failed to load some keys %v", len(errs))

	}
	numProcessed++
	log.Info("===== Processed %v", numProcessed)
	os.Remove(filePath)
	return nil
}

func jsonifyFile(rows []string) (map[string]interface{}, error) {

	var schema []string

	docs := make(map[string]interface{})
	colOffset := make([]int, 0)

	for i, row := range rows {

		// row 0 is the schema line
		value := make(map[string]interface{})
		if i == 0 {
			schema = strings.Split(row, ",")
			if len(schema) < 2 {
				return nil, fmt.Errorf("Invalid file format. Failed to parse schema line. Row %s", row)
			}

			for j, col := range schema {
				exclude := false
			innerLoop:
				for _, ec := range excludeCols {
					if col == ec {
						exclude = true
						break innerLoop
					}
				}
				if exclude == false {
					colOffset = append(colOffset, j)
				}
			}

		} else {
			// generate a json document from each row
			var key string
			colData := strings.Split(row, ",")
			if len(colData) != len(schema) {
				log.Warn("Mismatched schema Rows %v Schema %v", colData, schema)
			}

			// only jsonify the offsets that not part of the exclude list
			for _, offset := range colOffset {
				value[schema[offset]] = colData[offset]
			}

			// generate a unique for the data
			if value["timestamp"] == "" || value["pid"] == "" {
				log.Error("Values not found for timestamp or pid")
				continue
			}
			key = fmt.Sprintf("key-%v-%v-%d", value["pid"], value["timestamp"], i)
			marshalled, _ := json.MarshalIndent(value, "", "    ")
			docs[key] = marshalled
		}
	}
	return docs, nil
}

func loadKeys(bucket *couchbase.Bucket, docs map[string]interface{}) []error {

	errors := make([]error, 0)

	for key, value := range docs {
		added, err := bucket.AddRaw(key, 0, value.([]byte))
		if err != nil {
			log.Error("Failed to add key %v. Error %v", key, err)
			errors = append(errors, fmt.Errorf("Error adding key %v, Error %v", key, err))
		} else if added == false {
			errors = append(errors, fmt.Errorf("Key not added %v", key))
		}
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}
