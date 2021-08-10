package util

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goburrow/cache"
	goburrow "github.com/goburrow/cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type ConnectionCache struct {
	Provider goburrow.LoadingCache
}

func NewConnectionCache() *ConnectionCache {
	load := func(k goburrow.Key) (goburrow.Value, error) {
		address := fmt.Sprintf("%s", k)
		return NewConnectionPool(address), nil
	}
	remove := func(k cache.Key, v cache.Value) {
		if connectionPool, ok := v.(*ConnectionPool); ok {
			connectionPool.CloseAll()
		}
	}
	// Create a loading cache
	c := goburrow.NewLoadingCache(load,
		goburrow.WithMaximumSize(100),                  // Limit number of entries in the cache.
		goburrow.WithExpireAfterAccess(10*time.Minute), // Expire entries after 10 minutes since last accessed.
		goburrow.WithRefreshAfterWrite(20*time.Minute), // Expire entries after 20 minutes since last created.
		goburrow.WithRemovalListener(remove),
	)

	cc := &ConnectionCache{
		Provider: c,
	}

	return cc
}

func (cc *ConnectionCache) Get(address string) *Connection {
	if cpInterface, _ := cc.Provider.Get(address); cpInterface != nil {
		if cp, ok2 := cpInterface.(*ConnectionPool); ok2 {
			return cp.Get()
		}
	}
	return nil
}

func (cc *ConnectionCache) Put(c *Connection) {
	if cpInterface, _ := cc.Provider.Get(c.Address); cpInterface != nil {
		if cp, ok2 := cpInterface.(*ConnectionPool); ok2 {
			atomic.AddInt64(&c.Counter, -1)
			cp.PutIfHealthy(c)
		}
	}
}

func (cc *ConnectionCache) Close(c *Connection) {
	if cpInterface, _ := cc.Provider.Get(c.Address); cpInterface != nil {
		if cp, ok2 := cpInterface.(*ConnectionPool); ok2 {
			cp.Close(c)
		}
	}
}

// Func to init pool
func NewConnectionPool(address string) *ConnectionPool {
	cp := &ConnectionPool{}
	cp.Pool = &sync.Pool{
		New: func() interface{} {
			return cp.NewConnection(address)
		},
	}
	return cp
}

type ConnectionPool struct {
	Pool   *sync.Pool
	Closed bool
}

type Connection struct {
	Address string
	Conn    *grpc.ClientConn
	Counter int64
}

func (c *Connection) Close() {
	if c.Conn != nil {
		c.Conn.Close()
	}
}

func (cp *ConnectionPool) NewConnection(address string) *Connection {
	if cp.Closed {
		return nil
	}
	conn, err := grpc.Dial(address,
		grpc.WithInsecure(),
		grpc.WithTimeout(time.Duration(200)*time.Millisecond),
	)
	if err != nil {
		// This happens too frequently when scaling down
		// log.Printf("fail to dial to %v: %v\n", address, err)
		return nil
	}
	// log.Printf("New connection to: %v\n", address)
	// client := pb.NewVeriServiceClient(conn)
	return &Connection{
		Address: address,
		// Client:  client,
		Conn:    conn,
		Counter: 0,
	}
}

func (cp *ConnectionPool) Get() *Connection {
	c := cp.GetWithRetry(0)
	if c != nil {
		atomic.AddInt64(&c.Counter, 1)
		if atomic.LoadInt64(&c.Counter) <= 20 {
			cp.Put(c) // if there are less than 20 concurrent users put it back
		}
	}
	return c
}

func (cp *ConnectionPool) GetWithRetry(count int) *Connection {
	if count > 3 {
		return nil
	}
	connectionInterface := cp.Pool.Get()
	if connectionInterface == nil {
		return nil
	}
	if conn, ok := connectionInterface.(*Connection); ok {
		if conn != nil && conn.Conn != nil {
			return conn
		}
		count++
		return cp.GetWithRetry(count)
	}
	return nil
}

func (cp *ConnectionPool) Put(conn *Connection) {
	cp.Pool.Put(conn)
}

func (cp *ConnectionPool) PutIfHealthy(conn *Connection) {
	if conn != nil && conn.Conn != nil {
		if conn.Conn.GetState() == connectivity.Ready {
			cp.Pool.Put(conn)
		} else {
			err := conn.Conn.Close()
			if err != nil {
				log.Printf("Connection Close Error: %v\n", err.Error())
			}
		}
	}
}

func (cp *ConnectionPool) Close(conn *Connection) {
	if conn != nil && conn.Conn != nil && atomic.LoadInt64(&conn.Counter) <= 0 {
		err := conn.Conn.Close()
		if err != nil {
			log.Printf("Connection Close Error: %v\n", err.Error())
		}
	}
}

func (cp *ConnectionPool) CloseAll() {
	cp.Closed = true
	for i := 0; i < 1000; i++ { // Maximum 1000 connections will be closed
		connectionInterface := cp.Pool.Get()
		if connectionInterface == nil {
			return // Coonection pool is finished
		}
		if conn, ok := connectionInterface.(*Connection); ok {
			if conn != nil && conn.Conn != nil {
				err := conn.Conn.Close()
				if err != nil {
					log.Printf("Connection Close Error: %v\n", err.Error())
				}
			}
		} else {
			return
		}
	}
}
