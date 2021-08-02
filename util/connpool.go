package util

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/goburrow/cache"
	goburrow "github.com/goburrow/cache"
	grpcpool "github.com/processout/grpc-go-pool"
	"google.golang.org/grpc"
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
		if connectionPool, ok := v.(*grpcpool.Pool); ok {
			connectionPool.Close()
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

func (cc *ConnectionCache) Get(ctx context.Context, address string) *grpcpool.ClientConn {
	if cpInterface, _ := cc.Provider.Get(address); cpInterface != nil {
		if cp, ok2 := cpInterface.(*grpcpool.Pool); ok2 {
			for i := 0; i < 5; i++ {
				clientConn, err := cp.Get(ctx)
				if err == nil {
					return clientConn
				} else {
					log.Printf("Ignored error: %v\n", err)
				}
			}
		}
	}
	return nil
}

// Func to init pool
func NewConnectionPool(address string) *grpcpool.Pool {
	// init, capacity int, idleTimeout time.Duration,
	// maxLifeDuration ...time.Duration
	p, err := grpcpool.New(
		func() (*grpc.ClientConn, error) {
			return grpc.Dial(address,
				// grpc.WithBlock(),
				grpc.WithInsecure(),
				grpc.WithTimeout(time.Duration(200)*time.Millisecond),
			)
		},
		5,              // init
		1000,           // capacity
		5*time.Minute,  // idleTimeout
		30*time.Minute, // maxLifeDuration
	)
	if err != nil {
		return nil
	}
	return p
}
