# go-tools
Crontab tools for MySQL

## args

```
$ crontab_mysql -h
A crontab tool for MySQL, example:
crontab_mysql -host IP -user root -crontab '0 */5 * * * *' -files t.sql
  -crontab crontab
    	Execute at crontab (default "0 */5 * * * *")
  -database database
    	specify database (default "default")
  -files string
    	SQL files to execute: 1.sql,2.sql
  -help
    	help message
  -host host
    	MySQL host (default "127.0.0.1")
  -mention phone
    	Member phone: 158xxxx,136xxxx (default "@all")
  -password password
    	MySQL password for user
  -port port
    	MySQL port (default 3306)
  -ptinterval string
    	the format for partition name: [year|month|day] (default "month")
  -ptprefix string
    	the prefix of partition name (default "p")
  -pttable string
    	tables contains partition
  -queries SQL
    	SQL to run in MySQL, can run multiple SQL: select 1;select 1
  -user user
    	MySQL user (default "root")
  -webhook webhook
    	Through webhook to send warn message
```
