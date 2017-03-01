package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/kshvakov/clickhouse"
	"log"
	"strings"
	"time"
)

func main() {
	//Driver
	start_new := time.Now()
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}
	tx, err := connect.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO
	trackmanic2.events3 (id,  origin_id,  event_type,  invalid,  lead_type,
                                  cpc,  payout,  subid,  date,  timestamp,
                                  code_id,  ts_id,  campaign_id,  flow_id,  affiliate_id,
                                  affiliate_network_id,  offer_id,  code_type,  browser_name,
                                  browser_version,  platform,  operating_system,  is_robot,
                                  is_mobile,  accept_languages,  isp,  country,
                                  city,  region,  connection_type,  referer_host,
                                  ip_address,  client_document_height,  client_document_width,
                                  client_orientation_change,  client_operating_system,  client_window_top,
                                  client_browser_name,  client_timezone_offset,  client_touch_support,
                                  client_accelerometer,  client_fingerprint,  user_cookie,  c1,
                                  c2,  c3,  c4,  c5,  c6,  c7,  c8, c9,  c10,  node_hits)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`) // подготовим запрос
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 1000000; i++ {
		_, err = stmt.Exec(
			"111112328cd3e02c-83b1-41f0-8e29-c1681f03feb0",
			"1a9cbce7-0788-430e-b3e7-15bbbea2ac27",
			3,
			0,
			0,
			0.000010,
			0.200000,
			"",
			"2017-02-27",
			1488203857,
			4,
			3,
			3,
			4,
			0,
			0,
			4,
			0,
			"",
			"",
			"Unknown",
			"Linux Unknown",
			0,
			0,
			clickhouse.Array([]string{}),
			"",
			"",
			0,
			0,
			0,
			"",
			"127.0.0.1",
			0,
			0,
			0,
			"",
			0,
			"",
			0,
			0,
			0,
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			clickhouse.Array([]int32{}),
		) // <- пишем данные в буфер (блок с данными)
		if err != nil {
			log.Fatal(err)
		}
	}
	err = tx.Commit() // <- отправляем на сервер
	if err != nil {
		log.Fatal(err)
	}
	elapsed_new := time.Since(start_new)
	log.Printf("Driver took %s", elapsed_new)

	//--------------------HTTP
	start_new = time.Now()

	b := &bytes.Buffer{}   // creates IO Writer
	wr := csv.NewWriter(b) // creates a csv writer that uses the io buffer.
	for i := 0; i < 1000000; i++ {
		s := strings.Split(string("222228cd3e02c-83b1-41f0-8e29-c1681f03feb0,1a9cbce7-0788-430e-b3e7-15bbbea2ac27,3,0,0,0.000010,0.200000,,2017-02-27,1488203857,4,3,3,4,0,0,4,0,,,Unknown,Linux Unknown,0,0,[],,,0,0,0,,127.0.0.1,0,0,0,,0,,0,0,0,,,,,,,,,,,,,[]"), ",")
		err := wr.Write(s)
		if err != nil {
			log.Fatal(err)
		}

	}
	wr.Flush() // writes the csv writer data to the buffered data io writer(b(bytes.buffer))

	chc := GetClickhouseCluster([]string{"localhost:8123"})
	err = chc.ExecQuery(fmt.Sprintf("INSERT INTO trackmanic2.events3 FORMAT CSV %s", b.String()))
	if err != nil {
		log.Fatal(err)
	}

	elapsed_new = time.Since(start_new)
	log.Printf("Http took %s", elapsed_new)
}
