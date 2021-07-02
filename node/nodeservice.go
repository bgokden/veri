package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/bgokden/go-cache"
	pb "github.com/bgokden/veri/veriservice"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	grpcPeer "google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
)

// type VeriServiceServer interface {
// 	Search(context.Context, *SearchRequest) (*SearchResponse, error)
// 	Insert(context.Context, *InsertionRequest) (*InsertionResponse, error)
// 	Join(context.Context, *JoinRequest) (*JoinResponse, error)
// 	DataStream(*GetDataRequest, VeriService_DataStreamServer) error
// 	GetDataInfo(context.Context, *GetDataRequest) (*DataInfo, error)
// 	SearchStream(*SearchRequest, VeriService_SearchStreamServer) error
// 	InsertStream(VeriService_InsertStreamServer) error
// }

func (n *Node) Search(ctx context.Context, searchRequest *pb.SearchRequest) (*pb.SearchResponse, error) {
	return nil, nil
}

func (n *Node) Insert(ctx context.Context, insertionRequest *pb.InsertionRequest) (*pb.InsertionResponse, error) {
	config := insertionRequest.GetConfig()
	datum := insertionRequest.GetDatum()
	name := insertionRequest.GetDataName()
	dt, err := n.Dataset.Get(name)
	if err != nil {
		return nil, err
	}
	err = dt.Insert(datum, config)
	if err != nil {
		return nil, err
	}
	return &pb.InsertionResponse{Code: 0}, nil
}

func (n *Node) Join(ctx context.Context, joinRequest *pb.JoinRequest) (*pb.JoinResponse, error) {
	peer := joinRequest.GetPeer()
	n.AddPeerElement(peer)
	address := ""
	p, ok := grpcPeer.FromContext(ctx)
	if !ok {
		log.Printf("Peer can not be get from context %v\n", p)
	} else {
		index := strings.LastIndex(p.Addr.String(), ":")
		if index > 0 {
			address = p.Addr.String()[:index]
		} else {
			address = p.Addr.String()
		}
		ipAddress := net.ParseIP(strings.ReplaceAll(strings.ReplaceAll(address, "[", ""), "]", ""))
		if ipAddress != nil {
			if ipAddress.IsLoopback() { // problem with [::] interfaces
				address = "localhost"
			}
		}
		if len(address) <= 1 { // Keep it empty if it is not a reasonable ip
			address = ""
		}
	}
	return &pb.JoinResponse{Address: address}, nil
}

func (n *Node) DataStream(getDataRequest *pb.GetDataRequest, stream pb.VeriService_DataStreamServer) error {
	name := getDataRequest.GetName()
	dt, err := n.Dataset.Get(name)
	if err != nil {
		return err
	}
	datumStreamAll := make(chan *pb.Datum, 100)
	go func() {
		for datum := range datumStreamAll {
			// log.Printf("Send label: %v\n", string(datum.Value.Label))
			stream.Send(datum)
		}
	}()
	err = dt.StreamAll(datumStreamAll)
	if err != nil {
		return err
	}
	close(datumStreamAll)
	for datum := range datumStreamAll {
		// log.Printf("Flush - Send label: %v\n", string(datum.Value.Label))
		stream.Send(datum)
	}
	// log.Printf("DataStream finished\n")
	return nil
}

func (n *Node) GetDataInfo(ctx context.Context, getDataRequest *pb.GetDataRequest) (*pb.DataInfo, error) {
	name := getDataRequest.Name
	data, err := n.Dataset.Get(name)
	if err != nil {
		return nil, err
	}
	if data != nil {
		return data.GetDataInfo(), nil
	}
	return nil, nil
}

func (n *Node) SearchStream(searchRequest *pb.SearchRequest, stream pb.VeriService_SearchStreamServer) error {
	config := searchRequest.GetConfig()
	uid := config.GetUuid()
	if uid == "" {
		uid, err := uuid.NewRandom()
		if err != nil {
			return err
		}
		config.Uuid = uid.String()
	} else {
		err := n.QueryUUIDCache.Add(uid, true, cache.DefaultExpiration)
		if err != nil {
			if strings.Contains(err.Error(), "already exists") {
				return nil
			}
			return err
		}
	}
	aData, err := n.Dataset.GetNoCreate(config.GetDataName())
	if err != nil {
		return err
	}
	datumList := searchRequest.GetDatum()
	searchConfig := searchRequest.GetConfig()
	searchContext := searchRequest.GetContext()
	result, err := aData.MultiAggregatedSearch(datumList, searchConfig, searchContext)
	if err != nil {
		return err
	}
	// log.Printf("SearchStream: finished with len(%v)", len(result))
	for _, e := range result {
		// log.Printf("Send label: %v score: %v\n", string(e.Datum.Value.Label), e.Score)
		stream.Send(e)
	}
	// log.Printf("SearchStream: finished sending of len(%v)", len(result))
	return nil
}

func (n *Node) Listen() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", n.Port))
	if err != nil {
		log.Printf("failed to listen: %v", err)
		return err
	}

	grpcServer := grpc.NewServer(grpc.KeepaliveParams(keepalive.ServerParameters{
		// MaxConnectionIdle is a duration for the amount of time after which an
		// idle connection would be closed by sending a GoAway. Idleness duration is
		// defined since the most recent time the number of outstanding RPCs became
		// zero or the connection establishment.
		MaxConnectionIdle: 1 * time.Minute, // The current default value is infinity.
		// MaxConnectionAge is a duration for the maximum amount of time a
		// connection may exist before it will be closed by sending a GoAway. A
		// random jitter of +/-10% will be added to MaxConnectionAge to spread out
		// connection storms.
		MaxConnectionAge: 20 * time.Minute, // The current default value is infinity.
		// MaxConnectionAgeGrace is an additive period after MaxConnectionAge after
		// which the connection will be forcibly closed.
		MaxConnectionAgeGrace: 25 * time.Minute, // The current default value is infinity.
		// After a duration of this time if the server doesn't see any activity it
		// pings the client to see if the transport is still alive.
		// If set below 1s, a minimum value of 1s will be used instead.
		Time: 20 * time.Second, // The current default value is 2 hours.
		// After having pinged for keepalive check, the server waits for a duration
		// of Timeout and if no activity is seen even after that the connection is
		// closed.
		Timeout: 200 * time.Millisecond, // The current default value is 20 seconds.
	}), grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		// MinTime is the minimum amount of time a client should wait before sending
		// a keepalive ping.
		MinTime: 5 * time.Second, // The current default value is 5 minutes.
		// If true, server allows keepalive pings even when there are no active
		// streams(RPCs). If false, and client sends ping when there are no active
		// streams, server will send GOAWAY and close the connection.
		PermitWithoutStream: false, // false by default.
	}))

	pb.RegisterVeriServiceServer(grpcServer, n)
	reflection.Register(grpcServer)
	if err := grpcServer.Serve(lis); err != nil {
		log.Printf("failed to serve: %v", err)
		return err
	}
	return nil
}

func (n *Node) SendJoinRequest(id string) error {
	// log.Printf("(Call Join 0) Send Join request to %v", id)
	peerInfo := n.GetNodeInfo()
	request := &pb.JoinRequest{
		Peer: peerInfo,
	}
	conn := n.ConnectionCache.Get(id)
	if conn == nil {
		return errors.New("Connection failure")
	}
	defer n.ConnectionCache.Close(conn)
	// this connection should be closed time to time
	// It is observed that it can cause a split brain due to two nodes
	// sync to each other and never break connection
	client := conn.Client
	resp, err := client.Join(context.Background(), request)
	if err != nil {
		// log.Printf("(Call Join 2 %v => %v) There is an error %v", n.Port, id, err)
		return err
	}
	if resp.GetAddress() != "" {
		feedbackID := fmt.Sprintf("%v:%v", resp.GetAddress(), n.Port)
		// log.Printf("(Call Join 3 %v) Feedback ID: %v", n.Port, feedbackID)
		found := false
		for _, id := range n.KnownIds {
			if id == feedbackID {
				found = true
				break
			}
		}
		if !found {
			n.KnownIds = append(n.KnownIds, feedbackID)
		}
	}
	return nil
}

func (n *Node) CreateDataIfNotExists(ctx context.Context, in *pb.DataConfig) (*pb.DataInfo, error) {
	log.Printf("Config: %v\n", in)
	aData, err := n.Dataset.GetOrCreateIfNotExists(in)
	if err != nil {
		log.Printf("Error: %v\n", err)
		return nil, err
	}
	return aData.GetDataInfo(), nil
}

func (n *Node) getClient(address string) (pb.VeriServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithTimeout(time.Duration(200)*time.Millisecond))
	if err != nil {
		// log.Printf("fail to dial: %v\n", err)
		return nil, nil, err
	}
	client := pb.NewVeriServiceClient(conn)
	return client, conn, nil
}

func (n *Node) AddPeer(ctx context.Context, in *pb.AddPeerRequest) (*pb.AddPeerResponse, error) {
	return &pb.AddPeerResponse{}, n.AddPeerElement(in.GetPeer())
}

func (n *Node) SendAddPeerRequest(id string, peerInfo *pb.Peer) error {
	if FirstDifferent(peerInfo.GetAddressList(), []string{id}) == "" {
		return errors.New("Peer and target node is the same node")
	}
	// log.Printf("(Call Add Peer 0) Send Add Peer request to %v for (%v)", id, GetIdOfPeer(peerInfo))
	request := &pb.AddPeerRequest{
		Peer: peerInfo,
	}
	conn := n.ConnectionCache.Get(id)
	if conn == nil {
		return errors.New("Connection failure")
	}
	defer n.ConnectionCache.Put(conn)
	client := conn.Client
	_, err := client.AddPeer(context.Background(), request)
	if err != nil {
		// log.Printf("(Call Add Peer 2 %v => %v) There is an error %v", n.Port, id, err)
		return err
	}
	return nil
}

func (n *Node) Ping(ctx context.Context, in *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{}, nil
}

func (n *Node) SendPingRequest(id string) error {
	request := &pb.PingRequest{}
	conn := n.ConnectionCache.Get(id)
	if conn == nil {
		return errors.New("Connection failure")
	}
	defer n.ConnectionCache.Put(conn)
	client := conn.Client
	_, err := client.Ping(context.Background(), request)
	if err != nil {
		return err
	}
	return nil
}
