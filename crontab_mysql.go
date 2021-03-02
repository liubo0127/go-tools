package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/robfig/cron/v3"
)

var (
	host     string
	port     int
	user     string
	password string
	database string
	queries  string
	crontab  string
	help     bool
	files    string
	//print bool
	pttable    string
	ptprefix   string
	ptinterval string
	webhook    string
	mention    string
	service    string
	logfile    string
)

var logger *log.Logger

var stats int

func init() {
	flag.StringVar(&host, "host", "127.0.0.1", "MySQL `host`")
	flag.IntVar(&port, "port", 3306, "MySQL `port`")
	flag.StringVar(&user, "user", "root", "MySQL `user`")
	flag.StringVar(&password, "password", "", "MySQL `password` for user")
	flag.StringVar(&database, "database", "default", "specify `database`")
	flag.StringVar(&queries, "queries", "", "`SQL` to run in MySQL, can run multiple SQL: select 1;select 1")
	flag.StringVar(&crontab, "crontab", "0 */5 * * * *", "Execute at `crontab`")
	flag.BoolVar(&help, "help", false, "help message")
	flag.StringVar(&files, "files", "", "SQL files to execute: 1.sql,2.sql")
	flag.StringVar(&pttable, "pttable", "", "tables contains partition")
	flag.StringVar(&ptprefix, "ptprefix", "p", "the prefix of partition name")
	flag.StringVar(&ptinterval, "ptinterval", "month", "the format for partition name: [year|month|day]")
	flag.StringVar(&webhook, "webhook", "", "Through `webhook` to send warn message")
	flag.StringVar(&mention, "mention", "@all", "Member `phone`: 158xxxx,136xxxx")
	flag.StringVar(&service, "service", "", "name of mentioned `service`")
	flag.StringVar(&logfile, "logfile", "crontabMysql.log", "`logFile`")
	//flag.BoolVar(&print, "print", false, "Enable print query result")
	flag.Usage = usage

}

func usage() {
	fmt.Println("A crontab tool for MySQL, example:\ncrontab_mysql -host IP -user root -crontab '0 */5 * * * *' -files t.sql")
	flag.PrintDefaults()
}

func runAnySql(DB *sql.DB, query string) error {
	logger.Printf("[INFO] Execute SQL: %s", strings.ReplaceAll(query, "\n", " "))
	_, err := DB.Exec(query)
	if err != nil {
		logger.Printf("[FATAL] Execute SQL [%s] failed, error: %s\n", strings.ReplaceAll(query, "\n", " "), err.Error())
		qwWarn(query, err)
		return nil
	}

	return nil
}

func runQuery(DB *sql.DB, query string) (map[int]map[string]string, error) {
	logger.Printf("[INFO] Execute SQL: %s", strings.ReplaceAll(query, "\n", " "))
	result, err := DB.Query(query)
	if err != nil {
		logger.Printf("[FATAL] Execute SQL [%s] failed, error: %s\n", strings.ReplaceAll(query, "\n", " "), err.Error())
		qwWarn(query, err)
		return nil, nil
	}

	results := make(map[int]map[string]string)
	i := 0
	if true {
		cols, err := result.Columns()
		if err != nil {
			logger.Printf("[WARN] scan columns error: %s\n", err.Error())
		}

		values := make([][]byte, len(cols))
		scans := make([]interface{}, len(cols))

		for i := range scans {
			scans[i] = &values[i]
		}

		for result.Next() {
			if err := result.Scan(scans...); err != nil {
				logger.Printf("[WARN] scan data failed, error: %s\n", err.Error())
				continue
			}
			row := make(map[string]string)
			i++
			for idx, value := range values {
				key := cols[idx]
				row[key] = string(value)
			}
			results[i] = row
		}
	}
	return results, nil
}

func runFile(DB *sql.DB, file string) error {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		logger.Printf("[WARN] Read %s failed, error: %s", file, err.Error())
		qwWarn(file, err)
		return err
	}

	for _, qr := range strings.Split(string(content), ";") {
		if len(qr)-strings.Count(qr, " ")-strings.Count(qr, "\n") < 3 {
			continue
		}
		if err := runAnySql(DB, qr); err != nil {
			logger.Printf("[FATAL] Execute sql [%s] in %s failed, error: %s", strings.ReplaceAll(qr, "\n", " "), file, err.Error())
			qwWarn(fmt.Sprintf("%s(%s)", strings.ReplaceAll(qr, "\n", " "), file), err)
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
		logger.Printf("[FATAL] connection to mysql failed: %s\n", err.Error())
		qwWarn("Connect to mysql failed", nil)
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

type createPartition struct {
	user     string
	password string
	host     string
	port     int
	database string
	table    string
}

func (c createPartition) Run() {
	conn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", c.user, c.password, "tcp", c.host, c.port, c.database)
	DB, _ := sql.Open("mysql", conn)
	defer DB.Close()

	if err := DB.Ping(); err != nil {
		logger.Printf("[FATAL] connection to mysql failed: %s\n", err.Error())
		return
	}

	nowTime := time.Now()

	var (
		t1        string
		threshold string
		newPT     string
	)
	if ptinterval == "month" {
		t1 = "200601"
		threshold = fmt.Sprintf("%s-01 00:00:00", nowTime.AddDate(0, 2, 0).Format("2006-01"))
		newPT = fmt.Sprintf("%s%s", ptprefix, nowTime.AddDate(0, 1, 0).Format(t1))
	} else if ptinterval == "day" {
		t1 = "20060102"
		threshold = fmt.Sprintf("%s 00:00:00", nowTime.AddDate(0, 0, 2).Format("2006-01-02"))
		newPT = fmt.Sprintf("%s%s", ptprefix, nowTime.AddDate(0, 0, 1).Format(t1))
	} else if ptinterval == "year" {
		t1 = "2006"
		threshold = fmt.Sprintf("%s-01-01 00:00:00", nowTime.AddDate(2, 0, 0).Format("2006"))
		newPT = fmt.Sprintf("%s%s", ptprefix, nowTime.AddDate(1, 0, 0).Format(t1))
	}

	results, err := runQuery(DB, fmt.Sprintf("select PARTITION_NAME,PARTITION_EXPRESSION,CREATE_TIME from information_schema.partitions "+
		"where table_schema='%s' and table_name='%s' order by PARTITION_NAME desc limit 1;", c.database, c.table))
	if err != nil {
		logger.Printf("[FATAL] Get schema for %s.%s failed, error: %s", c.database, c.table, err.Error())
		return
	}
	logger.Printf("[INFO] Max partition is %s for %s.%s", results[1]["PARTITION_NAME"], c.database, c.table)

	//partition_expression := results[1]["PARTITION_EXPRESSION"]

	existMaxPT := results[1]["PARTITION_NAME"]

	if existMaxPT < newPT {
		logger.Printf("[INFO] Ready to add partition %s for %s.%s", newPT, c.database, c.table)
		sqlCMD := fmt.Sprintf("alter table %s add partition (partition %s values less than (unix_timestamp(\"%s\")*1000))", c.table, newPT, threshold)
		if err := runAnySql(DB, sqlCMD); err != nil {
			logger.Printf("[WARN] Add partition %s failed for %s.%s: error: %s", newPT, c.database, c.table, err.Error())
			return
		} else {
			logger.Printf("[INFO] Add partition %s successful for %s.%s!", newPT, c.database, c.table)
		}
	} else {
		logger.Printf("[INFO] %s.%s already have partition %s, skip add", c.database, c.table, newPT)
	}
}

func qwWarn(warn string, err error) {
	if webhook == "" {
		return
	}

	message := fmt.Sprintf("[%s]\nerror: %s", warn, err.Error())

	mentionList := strings.Split(mention, ",")
	length := len(mentionList)
	mentionStrList := ""
	for idx, mem := range mentionList {
		if idx+1 < length {
			mentionStrList += fmt.Sprintf("\"%s\",", mem)
		} else {
			mentionStrList += fmt.Sprintf("\"%s\"", mem)
		}
	}

	var jsonValue string
	if service != "" {
		jsonValue = fmt.Sprintf(`{"msgtype": "text", "text": {"content": "[%s]%s", "mentioned_mobile_list": [%s]}}`,
			service, message, mentionStrList)
	} else {
		jsonValue = fmt.Sprintf(`{"msgtype": "text", "text": {"content": "%s", "mentioned_mobile_list": [%s]}}`,
			message, mentionStrList)
	}
	resp, errI := http.Post(webhook, "application/json", bytes.NewBuffer([]byte(jsonValue)))

	if errI != nil {
		logger.Printf("[FATAL] %s", errI.Error())
	}

	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	logger.Printf("[INFO] Send monitor message, %s", body)
}

func main() {
	flag.Parse()
	if help {
		flag.Usage()
		return
	}

	logFile, err := os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		logger.Println(err.Error())
	}
	logger = log.New(logFile, "", log.Ldate|log.Ltime|log.Lshortfile)

	if err := requestMysql(user, password, host, port, database); err != nil {
		return
	}

	if (queries == "" && files == "") || (queries != "" && files != "") {
		fmt.Println("Please input sql through `--queries` or `--files`.")
		flag.Usage()
		return
	}

	defer qwWarn("Crontab job exit", nil)

	ct := cron.New(cron.WithSeconds())

	// crontab to run sql
	if _, err := ct.AddFunc(crontab, func() {
		if stats == 1 {
			return
		}
		stats = 1
		if err := requestMysql(user, password, host, port, database); err != nil {
			return
		}
	}); err != nil {
		logger.Printf("[FATAL] %s", err.Error())
		return
	}

	if pttable != "" {
		var spec string

		if ptinterval == "month" {
			spec = "0 0 * 3 * *"
		} else if ptinterval == "day" {
			spec = "0 3 * * * *"
		} else if ptinterval == "year" {
			spec = "0 0 * 3 1 *"
		} else {
			logger.Println("[INFO] --ptinterval must be one of month/year/day")
			fmt.Println("--ptinterval must be one of month/year/day")
			return
		}
		for _, table := range strings.Split(pttable, ",") {
			var (
				db, tb string
			)
			if strings.Contains(table, ".") {
				sp := strings.Split(table, ".")
				db = sp[0]
				tb = sp[1]
			} else {
				db = database
				tb = table
			}
			if _, err := ct.AddJob(spec, createPartition{
				user:     user,
				password: password,
				host:     host,
				port:     port,
				database: db,
				table:    tb,
			}); err != nil {
				logger.Printf("[FATAL] %s", err.Error())
				return
			}
		}
	}

	ct.Start()
	defer ct.Stop()
	select {}
}
