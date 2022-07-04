package main

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"runtime"
	"strings"
)

var result *ResStr

type ResStr struct {
	chReq chan string
	chRes chan []string
}

func NewCH() *ResStr {
	out := new(ResStr)
	out.chReq = make(chan string)
	out.chRes = make(chan []string)
	return out
}
func (request *ResStr) Run() {
	var tab string
	for {
		tab = <-request.chReq
		request.chRes <- request.query(tab)
	}
}
func init() {
	result = NewCH()
	go result.Run()
}
func (resstr *ResStr) query(tab string) []string {
	producer_db, _ := sql.Open("mysql", "root:rootpass@tcp(10.0.56.87:4306)/xx")
	defer producer_db.Close()
	tmpsql := "select * from " + tab + " limit 100"
	rows, err := producer_db.Query(tmpsql)
	defer rows.Close()
	handleError(err)
	cols, err := rows.Columns()
	handleError(err)
	t_col := strings.Join(cols[:], ",")
	t_sql := "insert into " + tab + "(" + t_col + ") values ("
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]interface{}, len(values))
	var results []string
	i := 0
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		handleError(err)
		d_sql := bytes.Buffer{}
		d_sql.WriteString(t_sql)
		row := make(map[string]string)
		for k, v := range values {
			key := cols[k]
			row[key] = string(v)
		}
		for i, col := range cols {
			d_sql.WriteString("'" + row[col] + "'")
			if i != len(cols)-1 {
				d_sql.WriteString(",")
			}
		}
		d_sql.WriteString(")")
		results = append(results, d_sql.String())
		i++
	}
	if err = rows.Err(); err != nil {
		fmt.Println(err.Error())
	}
	return results
}
func handleError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func migrate(tab string) {
	result.chReq <- tab
	res_arr := <-result.chRes
	const (
		GOROUTINE_COUNT = 5
	)
	TASK_COUNT := len(res_arr)
	chTas := make(chan string, GOROUTINE_COUNT)
	chTasR := make(chan int, GOROUTINE_COUNT)
	target_db, _ := sql.Open("mysql", "root:rootpass@tcp(10.0.56.87:5306)/xx")
	defer target_db.Close()
	for i := 0; i < GOROUTINE_COUNT; i++ {
		go func() {
			for {
				url := <-chTas
				fmt.Println(url)
				tx1, _ := target_db.Begin()
				_, err := tx1.Exec(url)
				handleError(err)
				tx1.Commit()
				chTasR <- 0
			}
		}()
	}
	go func() {
		urls := make([]string, TASK_COUNT)
		for i := 0; i < TASK_COUNT; i++ {
			urls[i] = res_arr[i]
		}
		for i := 0; i < TASK_COUNT; i++ {
			chTas <- urls[i]
		}
	}()
	for i := 0; i < TASK_COUNT; i++ {
		d := <-chTasR
		_ = d
	}
}
func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	tab := "xx.xx"
	migrate(tab)
}
