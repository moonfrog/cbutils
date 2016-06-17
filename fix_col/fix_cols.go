package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"runtime"
	//	"strconv"
	"sync"

	"github.com/jeffail/tunny"
	_ "github.com/lib/pq"
)

var dbName = flag.String("dbName", "", "Database name")
var dbUser = flag.String("dbUser", "", "Database username ")
var dbPort = flag.Int("dbPort", 5439, "Database port")
var dbHost = flag.String("dbHost", "", "Database host")
var dbPass = flag.String("dbPass", "", "Database password")
var schema = flag.String("schema", "public", "Schema name")
var col = flag.String("column", "pid", "column name to fix")
var threads = flag.Int("threads", 2, "number of threads")

var wg sync.WaitGroup

type work struct {
	table  string
	column string
	schema string
	db     *sql.DB
}

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	if *dbName == "" || *dbUser == "" || *dbPort == 0 || *dbHost == "" || *dbPass == "" {
		log.Fatalf("Incorrect credentials")
	}

	url := fmt.Sprintf("sslmode=require user=%v password=%v host=%v port=%v dbname=%v "+
		"connect_timeout=0",
		*dbUser,
		*dbPass,
		*dbHost,
		*dbPort,
		*dbName)

	db, err := sql.Open("postgres", url)
	if err != nil {
		log.Fatalf("Open db error %v", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("Connection not established: url %v, Error %v", url, err)
	}

	wp, _ := tunny.CreatePool(*threads, doColumnFix).Open()

	// get a list of all tables in the schema
	listTableStmt := fmt.Sprintf("select distinct tablename from pg_table_def pd where pd.schemaname = '%s' and tablename like '%%m_table_%%';", *schema)

	tableList := make([]string, 0)

	rows, err := db.Query(listTableStmt)
	if err != nil {
		log.Fatalf(" Query %v Failed %v", listTableStmt, err)
	}

	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			log.Fatalf(" Failed to Scan rows %v", err)
		}

		tableList = append(tableList, tableName)
	}

	if len(tableList) == 0 {
		log.Printf("No tables to scan ")
	}

	for _, table := range tableList {

		if table == "m_table_adjust" || table == "m_table_apsalar" {
			continue
		}

		w := &work{table: table, column: *col, schema: *schema, db: db}

		go func(w *work) {
			wg.Add(1)
			wp.SendWork(w)
			log.Printf(" Done table %v", w.table)
		}(w)

	}

	// irons in the fire, wait for them to finish
	wg.Wait()

}

var DescribeQuery = `SELECT /* fetching column descriptions for table */ "column",
       TYPE,
       encoding,
       distkey,
       sortkey,
       ad.adsrc
FROM pg_table_def de,
     pg_attribute at
  LEFT JOIN pg_attrdef ad ON (at.attrelid,at.attnum) = (ad.adrelid,ad.adnum)
WHERE de.schemaname = '%s'
AND   de.tablename = '%s'
AND   at.attrelid = '%s.%s'::regclass
AND   de.column = at.attname;`

func doColumnFix(arg interface{}) interface{} {
	defer wg.Done()

	var retErr error

	w := arg.(*work)

	//get the table description
	describeQuery := fmt.Sprintf(DescribeQuery, w.schema, w.table, w.schema, w.table)

	rows, err := w.db.Query(describeQuery)
	if err != nil {
		log.Printf("Failed to execute query. Error %v", err)
		return err
	}

	var createTable string
	var sortKeys string

	var selectCols string

	for rows.Next() {
		var column string
		var colType string
		var encoding string
		var distKey bool
		var sortKey int
		var adSrc interface{}

		var row string
		var sc string

		if createTable == "" {
			createTable = "("
		} else {
			createTable = createTable + ","
		}
		err = rows.Scan(&column, &colType, &encoding, &distKey, &sortKey, &adSrc)
		if err != nil {
			log.Printf(" Failed to Scan rows %v", err)
			return err
		}

		if column == w.column {
			if colType == "bigint" {
				// already converted. Skip
				return nil
			}
			// this column needs to be changed to bigint
			row = fmt.Sprintf("%s bigint", column)
			sc = fmt.Sprintf("convert(bigint, %s.%s)", w.table, column)
		} else {
			row = fmt.Sprintf("%s %s", column, colType)
			sc = w.table + "." + column
		}

		if selectCols == "" {
			selectCols = sc
		} else {
			selectCols = fmt.Sprintf("%s, %s", selectCols, sc)
		}

		// is this the distkey ?
		if distKey == true {
			row = fmt.Sprintf("%s DISTKEY", row)
		}

		// append the encoding
		if encoding != "none" {
			row = fmt.Sprintf("%s ENCODE %s", row, encoding)
		}

		if adSrc != nil {
			// ascii value. covert to int and subtract 48
			row = fmt.Sprintf("%s DEFAULT %v", row, int(adSrc.([]uint8)[0])-48)
		}

		createTable = fmt.Sprintf("%s \r\n %s", createTable, row)
		if sortKey != 0 {
			if sortKeys == "" {
				sortKeys = fmt.Sprintf("SORTKEY (%s", column)
			} else {
				sortKeys = fmt.Sprintf("%s, %s", sortKeys, column)
			}
		}
	}

	createTable = fmt.Sprintf("%s\r\n)", createTable)
	createTable = fmt.Sprintf("%s %s )", createTable, sortKeys)

	newTable := w.table + "_temp"

	createTableQuery := fmt.Sprintf("begin; lock table %v; CREATE table %s %s", w.table, newTable, createTable)
	//log.Printf(" Create Table Query %v", createTableQuery)

	_, err = w.db.Exec(createTableQuery)
	if err != nil {
		log.Fatalf("====== Failed to execute query %v", err)
	} else {

		defer func() {
			if retErr != nil {
				w.db.Exec("abort")
			}
		}()

		var insertStmt string

		if w.table == "m_table_apsalar_installs" ||
			w.table == "m_table_user_staging" ||
			w.table == "m_table_user" {
			insertStmt = fmt.Sprintf("insert into %s select %s from %s limit 1", newTable, selectCols, w.table)
		} else {
			insertStmt = fmt.Sprintf("insert into %s select %s from %s where (left(%s,6) between 0 and 999999) and (right(%s,6) between 0 and 999999)", newTable, selectCols, w.table, w.column, w.column)
		}

		//log.Printf(" insert statement %v", insertStmt)

		_, err = w.db.Exec(insertStmt)
		if err != nil {
			log.Printf(" ******* Failed %v Table %v Query %v Query %v", err, w.table, createTableQuery, insertStmt)
			retErr = err
			return err
		}

		dropTable := fmt.Sprintf("drop table %v cascade", w.table)
		_, err = w.db.Exec(dropTable)
		if err != nil {
			log.Printf(" Failed %v %v", dropTable, err)
			retErr = err
			return err
		}

		alterTable := fmt.Sprintf("alter table %s.%s rename to %s", w.schema, newTable, w.table)
		_, err = w.db.Exec(alterTable)
		if err != nil {
			log.Printf(" Failed %v %v", alterTable, err)
			retErr = err
			return err
		}

		_, err = w.db.Exec("commit")
		if err != nil {
			log.Printf("Commit failed ")
			return err
		}

	}

	return nil
}
