package models

import (
	pb "github.com/bgokden/veri/veriservice"
	"google.golang.org/grpc"
)

type Peer struct {
	Address   string
	Version   string
	Avg       []float64
	Hist      []float64
	N         int64
	Timestamp int64
}

type Client struct {
	Address string
	Client  *pb.VeriServiceClient
	Conn    *grpc.ClientConn
}
