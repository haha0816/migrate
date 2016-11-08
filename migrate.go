package main

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strings"
)

type Hostinfo struct {
	DBUser,
	DBPassword,
	DBname,
	DBHost,
	DBPort,
	DBChar string
}

var done = make(chan bool)
var chRes = make(chan map[string]string)
var chCol = make(chan []string)

func (host *Hostinfo) get_conn(ip, port string) (*sql.DB, error) {
	host.DBUser = "root"
	host.DBPassword = "rootpass"
	host.DBname = "test"
	host.DBHost = ip
	host.DBPort = port
	host.DBChar = "utf8"
	if host.DBHost != "" {
		host.DBHost = "tcp(" + host.DBHost + ":" + host.DBPort + ")"
	}
	db, err := sql.Open("mysql", host.DBUser+":"+host.DBPassword+"@"+host.DBHost+"/"+host.DBname+"?charset="+host.DBChar)
	return db, err
}

type Sourceinfo struct {
	Sourceinfo Hostinfo
}
type Targetinfo struct {
	Targetinfo Hostinfo
}

func (source_h *Sourceinfo) get_data(chRes chan<- map[string]string) {
	fmt.Println("get_data...")
	tmp_sql := "select * from jdorders_4.orders_14 limit 3"
	myDB, _ := source_h.Sourceinfo.get_conn("10.0.56.87", "4306")
	defer myDB.Close()
	rows, err := myDB.Query(tmp_sql)
	defer rows.Close()
	handleError(err)
	cols, err := rows.Columns()
	chCol <- cols
	handleError(err)
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]interface{}, len(values))
	//results := make(map[int]map[string]string)
	i := 0
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		handleError(err)
		row := make(map[string]string)
		for k, v := range values {
			key := cols[k]
			row[key] = string(v)
		}
		//results[i] = row
		chRes <- row
		i++
	}
	if err = rows.Err(); err != nil {
		fmt.Println(err.Error())
	}
	done <- true
}

func (target *Targetinfo) insert(sql string) {
	db, _ := target.Targetinfo.get_conn("10.0.56.87", "5306")
	defer db.Close()
	fmt.Println(sql)
	result, err := db.Exec(sql)
	handleError(err)
	fmt.Println(result.LastInsertId())
}
func (target *Targetinfo) get_sql(chRes <-chan map[string]string) {
	fmt.Println("consumer...")
	col_arr := <-chCol
	t_col := strings.Join(col_arr[:], ",")
	t_sql := "insert into jdorders_4.orders_14(" + t_col + ") values ("
	for {
		c_item := <-chRes
		//fmt.Println(c_item)
		d_sql := bytes.Buffer{}
		d_sql.WriteString(t_sql)
		for i, col := range col_arr {
			d_sql.WriteString("'" + c_item[col] + "'")
			if i != len(col_arr)-1 {
				d_sql.WriteString(",")
			}
		}
		d_sql.WriteString(")")
		target.insert(d_sql.String())
	}
}

func handleError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func migrate() {
	t_producer := &Sourceinfo{}
	t_consumer := &Targetinfo{}
	go func() {
		for {
			t_producer.get_data(chRes)
		}
	}()
	go t_consumer.get_sql(chRes)
	<-done
}
func main() {
	migrate()
}
