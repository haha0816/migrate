ackage main

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

func (source_h *Sourceinfo) get_data() []string {
    tmp_sql := "select * from xx.xx"
    myDB, _ := source_h.Sourceinfo.get_conn("xx", "4306")
    defer myDB.Close()
    rows, err := myDB.Query(tmp_sql)
    defer rows.Close()
    handleError(err)
    cols, err := rows.Columns()
    handleError(err)
    t_col := strings.Join(cols[:], ",")
    t_sql := "insert into xx.xx(" + t_col + ") values ("
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
func (target *Targetinfo) do_sql(chRes <-chan []string) {
    results := <-chRes
    db, _ := target.Targetinfo.get_conn("xx", "5306")
    defer db.Close()
    for _, sql := range results {
        tx1, _ := db.Begin()
        _, err := tx1.Exec(sql)
        handleError(err)
        tx1.Commit()
    }
}

func handleError(err error) {
    if err != nil {
        log.Fatal(err)
    }
}
func migrate() {
    var chRes = make(chan []string)
    t_producer := &Sourceinfo{}
    t_consumer := &Targetinfo{}

    go func() {
        chRes <- t_producer.get_data()
    }()
    t_consumer.do_sql(chRes)
}
func main() {
    migrate()
}
