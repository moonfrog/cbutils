package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"time"

	"github.com/couchbase/go-couchbase"
	"github.com/moonfrog/badger/common"
	"github.com/moonfrog/badger/zootils"
)

type RedshiftConfig struct {
	DbUser     string // Redshift username
	DbPassword string // Redshift password
	DbHost     string // Redshift host
	DbPort     string // Redshift database port
	DbName     string // Redshift database name
}

type CouchbaseConfig struct {
	Host      string
	Port      string
	QueryPort string
	Bucket    string
}

type tableSize struct {
	TableName string `json:"tableName"`
	Size      int    `json:"sizeMb"`
	Rows      uint64 `json"rows"`
}

type SizeRow struct {
	Timestamp  string       `json:"timestamp"`
	TableSizes []*tableSize `json:"tableSizeList"`
}

const SIZE_QUERY = `select 
    trim(a.name) as Table,
    b.mbytes,
    a.rows
from (
    select db_id, id, name, sum(rows) as rows
    from stv_tbl_perm a
    group by db_id, id, name
) as a
join pg_class as pgc on pgc.oid = a.id
join pg_namespace as pgn on pgn.oid = pgc.relnamespace
join pg_database as pgdb on pgdb.oid = a.db_id
join (
    select tbl, count(*) as mbytes
    from stv_blocklist
    group by tbl
) b on a.id = b.tbl
order by mbytes desc, a.db_id, a.name`

func main() {

	var config RedshiftConfig
	var tsList = make([]*tableSize, 0)

	common.Init("redshift", 3000)
	err := zootils.GetInstance().LoadConfig(&config, "config/redshiftProduction", func(string) {})

	if err != nil {
		log.Fatalf("Couldn't load config. Err - %s", err)

	}

	if config.DbName == "" || config.DbUser == "" || config.DbHost == "" ||
		config.DbPassword == "" || config.DbPort == "" {
		log.Fatalf("Config error %v", config)
	}

	var cbConfig CouchbaseConfig
	err = zootils.GetInstance().LoadConfig(&cbConfig, "config/couchbase", func(string) {})

	if err != nil {
		log.Fatalf("Couldn't load config. Err - %s", err)

	}

	if cbConfig.Host == "" || cbConfig.Port == "" || cbConfig.QueryPort == "" ||
		cbConfig.Bucket == "" {
		log.Fatalf("Config error %v", cbConfig)
	}

	url := fmt.Sprintf("sslmode=require user=%v password=%v host=%v port=%v dbname=%v "+
		"connect_timeout=0",
		config.DbUser,
		config.DbPassword,
		config.DbHost,
		config.DbPort,
		config.DbName)

	db, err := sql.Open("postgres", url)
	if err != nil {
		log.Fatalf("Open db error %v", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("Connection not established: url %v, Error %v", url, err)
	}

	startTime := time.Now()
	rows, err := db.Query(SIZE_QUERY)
	queryTime := time.Now().Sub(startTime).Seconds()

	if err != nil {
		log.Fatalf("Failed to execute query %v", err)
	}

	log.Printf("Query execution successfull. Tool %v seconds", queryTime)
	for rows.Next() {
		ts := &tableSize{}
		err = rows.Scan(&ts.TableName, &ts.Size, &ts.Rows)
		if err != nil {
			log.Fatalf(" Failed to Scan rows %v", err)
		}

		tsList = append(tsList, ts)
	}

	rows.Close()
	db.Close()

	row := &SizeRow{Timestamp: fmt.Sprintf("%s", startTime.Format("Mon Jan _2 15:04:05 2006")),
		TableSizes: tsList}

	encoded, _ := json.MarshalIndent(row, "", "    ")

	// Store this data into the couchbase node
	c, err := couchbase.Connect("http://" + cbConfig.Host + ":" + cbConfig.Port + "/")
	if err != nil {
		log.Fatalf("Error connecting:  %v", err)
	}

	pool, err := c.GetPool("default")
	if err != nil {
		log.Fatalf("Error getting pool:  %v", err)
	}

	bucket, err := pool.GetBucket(cbConfig.Bucket)
	if err != nil {
		log.Fatalf("Error getting bucket:  %v", err)
	}

	key := fmt.Sprintf("stats_%v", startTime.Unix())
	err = bucket.SetRaw(key, 0, encoded)
	if err != nil {
		log.Fatalf("Unable to set %v", err)
	}

}
