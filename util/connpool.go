package util

import (
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/bgokden/veri/veriservice"
	goburrow "github.com/goburrow/cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
)

type ConnectionCache struct {
	Provider goburrow.LoadingCache
}

func NewConnectionCache() *ConnectionCache {
	load := func(k goburrow.Key) (goburrow.Value, error) {
		address := fmt.Sprintf("%s", k)
		return NewConnectionPool(address), nil
	}
	// Create a loading cache
	c := goburrow.NewLoadingCache(load,
		goburrow.WithMaximumSize(100),                  // Limit number of entries in the cache.
		goburrow.WithExpireAfterAccess(10*time.Minute), // Expire entries after 1 minute since last accessed.
		goburrow.WithRefreshAfterWrite(20*time.Minute), // Expire entries after 2 minutes since last created.
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
			cp.PutIfHealthy(c)
		}
	}
}

// Func to init pool
func NewConnectionPool(address string) *ConnectionPool {
	pool := &sync.Pool{
		New: func() interface{} {
			return NewConnection(address)
		},
	}
	return &ConnectionPool{
		Pool: pool,
	}
}

type ConnectionPool struct {
	Pool *sync.Pool
}

type Connection struct {
	Address string
	Client  pb.VeriServiceClient
	Conn    *grpc.ClientConn
}

func (c *Connection) Close() {
	if c.Conn != nil {
		c.Conn.Close()
	}
}

func NewConnection(address string) *Connection {
	conn, err := grpc.Dial(address,
		grpc.WithBlock(),
		grpc.WithInsecure(),
		grpc.WithTimeout(time.Duration(1000)*time.Millisecond),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			// After a duration of this time if the client doesn't see any activity it
			// pings the server to see if the transport is still alive.
			// If set below 10s, a minimum value of 10s will be used instead.
			Time: 600 * time.Second, // The current default value is infinity.
			// After having pinged for keepalive check, the client waits for a duration
			// of Timeout and if no activity is seen even after that the connection is
			// closed.
			Timeout: 20 * time.Second, // The current default value is 20 seconds.
			// If true, client sends keepalive pings even with no active RPCs. If false,
			// when there are no active RPCs, Time and Timeout will be ignored and no
			// keepalive pings will be sent.
			PermitWithoutStream: false, // false by default.
		}))
	if err != nil {
		// This happens too frequently when scaling down
		// log.Printf("fail to dial to %v: %v\n", address, err)
		return nil
	}
	// log.Printf("New connection to: %v\n", address)
	client := pb.NewVeriServiceClient(conn)
	return &Connection{
		Address: address,
		Client:  client,
		Conn:    conn,
	}
}

func (cp *ConnectionPool) Get() *Connection {
	return cp.GetWithRetry(0)
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
			if conn.Conn.GetState() == connectivity.Ready {
				return conn
			} else {
				err := conn.Conn.Close()
				if err != nil {
					log.Printf("Connection Close Error: %v\n", err.Error())
				}

			}
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
