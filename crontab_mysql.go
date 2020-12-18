package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/robfig/cron/v3"

	_ "github.com/go-sql-driver/mysql"
)

var (
	host     string
	port     int
	user     string
	password string
	database string
	queries  string
	crontab  string
	//Concurrent bool
	help  bool
	files string
	//print bool
)

var logger *log.Logger

var stats int

func init() {
	flag.StringVar(&host, "host", "127.0.0.1", "MySQL `host`")
	flag.IntVar(&port, "port", 3306, "MySQL `port`")
	flag.StringVar(&user, "user", "root", "MySQL `user`")
	flag.StringVar(&password, "password", "null", "MySQL `password` for user")
	flag.StringVar(&database, "database", "default", "specify `database`")
	flag.StringVar(&queries, "queries", "", "`SQL` to run in MySQL, can run multiple SQL: select 1;select 1")
	flag.StringVar(&crontab, "crontab", "0 */5 * * * *", "Execute at `crontab`")
	flag.BoolVar(&help, "help", false, "help message")
	//flag.BoolVar(&Concurrent, "concurrent", false, "Enable concurrent for all queries")
	flag.StringVar(&files, "files", "", "SQL files to execute: 1.sql,2.sql")
	//flag.BoolVar(&print, "print", false, "Enable print query result")

	logFile, err := os.OpenFile("crontabMysql.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		logger.Fatal(err)
	}
	logger = log.New(logFile, "TRACE: ", log.Ldate|log.Lmicroseconds|log.Lshortfile)
}

func runAnySql(DB *sql.DB, query string) error {
	_, err := DB.Exec(query)
	if err != nil {
		fmt.Printf("Execute SQL [%s] failed, error: %s\n", query, err.Error())
		logger.Printf("[FATAL] Execute SQL [%s] failed, error: %s\n", query, err.Error())
		return nil
	}

	return nil
}

func runQuery(DB *sql.DB, query string) error {
	result, err := DB.Query(query)
	if err != nil {
		fmt.Printf("Execute SQL [%s] failed, error: %s\n", query, err.Error())
		logger.Printf("[FATAL] Execute SQL [%s] failed, error: %s\n", query, err.Error())
		return nil
	}

	if true {
		cols, err := result.Columns()
		if err != nil {
			log.Printf("[WARN] scan columns error: %s\n", err.Error())
		}

		fmt.Println()
		for _, col := range cols {
			fmt.Printf("%s ", col)
		}
		fmt.Println()

		values := make([][]byte, len(cols))
		scans := make([]interface{}, len(cols))

		for i := range scans {
			scans[i] = &values[i]
		}
		for result.Next() {
			if err := result.Scan(scans...); err != nil {
				log.Printf("[WARN] scan data failed, error: %s\n", err.Error())
			}

			for _, value := range values {
				fmt.Printf("%s ", string(value))
			}
			fmt.Println()
		}
	}
	return nil
}

func runFile(DB *sql.DB, file string) error {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Printf("Read %s failed, error: %s", file, err.Error())
		logger.Printf("[WARN] Read %s failed, error: %s", file, err.Error())
		return err
	}

	for _, qr := range strings.Split(string(content), ";") {
		if len(qr)-strings.Count(qr, " ")-strings.Count(qr, "\n") < 3 {
			continue
		}
		if err := runAnySql(DB, qr); err != nil {
			fmt.Printf("Execute sql [%s] in %s failed, error: %s", qr, file, err.Error())
			logger.Printf("[FATAL] Execute sql [%s] in %s failed, error: %s", qr, file, err.Error())
			return err
		}
	}
	return nil

}

func requestMysql(user, password, host string, port int, database string) error {
	conn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", user, password, "tcp", host, port, database)
	DB, _ := sql.Open("mysql", conn)
	defer DB.Close()
	defer func() { stats = 0 }()

	if err := DB.Ping(); err != nil {
		fmt.Printf("connection to mysql failed: %s\n", err.Error())
		logger.Printf("connection to mysql failed: %s\n", err.Error())
		return err
	}
	if queries != "" {
		for _, query := range strings.Split(queries, ";") {
			if query != "" {
				if err := runAnySql(DB, query); err != nil {
					return err
				}
			}
		}
	}

	if files != "" {
		for _, file := range strings.Split(files, ",") {
			if file != "" {
				if err := runFile(DB, file); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func main() {
	flag.Parse()
	if help {
		flag.Usage()
		return
	}

	if (queries == "" && files == "") || (queries != "" && files != "") {
		fmt.Println("Please input sql through `--queries` or `--files`.")
		logger.Println("Please input sql through `--queries` or `--files`.")
		flag.Usage()
		return
	}

	ct := cron.New(cron.WithSeconds())

	num := 0
	if _, err := ct.AddFunc(crontab, func() {
		if stats == 1 {
			return
		}
		stats = 1
		num++
		logger.Printf("[INFO] Execute the %d times", num)
		if err := requestMysql(user, password, host, port, database); err != nil {
			return
		}
	}); err != nil {
		logger.Printf("[FATAL] %s", err.Error())
		return
	}

	ct.Start()
	defer ct.Stop()
	select {}
}
