package util

import (
	"log"
	"sync"
	"time"

	pb "github.com/bgokden/veri/veriservice"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
)

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
		grpc.WithTimeout(time.Duration(200)*time.Millisecond),
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
		log.Printf("fail to dial: %v\n", err)
		return nil
	}
	log.Printf("New connection to: %v\n", address)
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
	if count > 5 {
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
