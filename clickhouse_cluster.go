package main

import (
	"time"
	"sync"
	"sync/atomic"
	"github.com/roistat/go-clickhouse"
	"errors"
	"log"
)

type ClickhouseCluster struct {
	chCluster		*clickhouse.Cluster
	running			int32
	hosts			[]string
	wg 			sync.WaitGroup
}

func (c *ClickhouseCluster) IsRunning() bool {
	return atomic.LoadInt32(&c.running) != 0
}

func (c *ClickhouseCluster) Shutdown() {
	atomic.StoreInt32(&c.running, 0)

	c.wg.Wait()
}

func (c *ClickhouseCluster) GetConnection() (*clickhouse.Conn, error) {
	if c.IsRunning() {
		return c.chCluster.ActiveConn(), nil
	}
	return nil, errors.New("ClickhouseCluster is not running")
}

func (c *ClickhouseCluster) ExecQuery(stmt string, args ...interface{}) error {
	if c.IsRunning() {
		query := clickhouse.NewQuery(stmt, args...)
		return query.Exec(c.chCluster.ActiveConn())
	}
	return errors.New("ClickhouseCluster is not running")
}

func (c *ClickhouseCluster) Run() {
	c.running = 1 // init status atomic, no atomic access needed here, yet

	chConnections := []*clickhouse.Conn{}

	chTransport := clickhouse.NewHttpTransport()
	for _, chInstance := range c.hosts {
		chConnections = append(chConnections, clickhouse.NewConn(chInstance, chTransport))
	}

	c.chCluster = clickhouse.NewCluster(chConnections...)
	c.chCluster.OnCheckError(func (conn *clickhouse.Conn) {
		log.Printf("Clickhouse connection failed %s", conn.Host)
	})

	c.chCluster.Check()
	c.wg.Add(1)
	go func() {
		for {
			if !c.IsRunning() {
				break
			}

			c.chCluster.Check()
			time.Sleep(time.Second)
		}

		c.wg.Done()
	}()
}

func NewClickhouseCluster(hosts []string) *ClickhouseCluster {
	chc := ClickhouseCluster{hosts:hosts}
	return &chc
}

var chc_once sync.Once
var chc_instance *ClickhouseCluster

func GetClickhouseCluster(hosts []string) *ClickhouseCluster {
	chc_once.Do(func() {
		chc_instance = NewClickhouseCluster(hosts)
		chc_instance.Run()
	})
	return chc_instance
}