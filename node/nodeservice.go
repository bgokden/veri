package node

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"

	pb "github.com/bgokden/veri/veriservice"
	"google.golang.org/grpc"
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
	n.AddPeer(peer)
	address := ""
	p, ok := grpcPeer.FromContext(ctx)
	if !ok {
		log.Printf("Peer can not be get from context %v\n", p)
	} else {
		address = strings.Split(p.Addr.String(), ":")[0]
		if len(address) <= 1 { // problem with [::] interfaces
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
	log.Printf("DataStream finished\n")
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
	aData, err := n.Dataset.Get(config.GetDataName())
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
	log.Printf("SearchStream: finished with len(%v)", len(result))
	for _, e := range result {
		// log.Printf("Send label: %v score: %v\n", string(e.Datum.Value.Label), e.Score)
		stream.Send(e)
	}
	log.Printf("SearchStream: finished sending of len(%v)", len(result))
	return nil
}

func (n *Node) Listen() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", n.Port))
	if err != nil {
		log.Printf("failed to listen: %v", err)
		return err
	}

	grpcServer := grpc.NewServer()
	pb.RegisterVeriServiceServer(grpcServer, n)
	reflection.Register(grpcServer)
	if err := grpcServer.Serve(lis); err != nil {
		log.Printf("failed to serve: %v", err)
		return err
	}
	return nil
}

func (n *Node) SendJoinRequest(id string) error {
	peerInfo := n.GetNodeInfo()
	request := &pb.JoinRequest{
		Peer: peerInfo,
	}
	client, _, err := n.getClient(id)
	if err != nil {
		log.Printf("(Call Join 1 %v) There is an error %v", n.Port, err)
		return err
	}
	resp, err := client.Join(context.Background(), request)
	if err != nil {
		log.Printf("(Call Join 2 %v => %v) There is an error %v", n.Port, id, err)
		return err
	}
	if resp.GetAddress() != "" {
		feedbackId := fmt.Sprintf("%v:%v", resp.GetAddress(), n.Port)
		found := false
		for _, id := range n.KnownIds {
			if id == feedbackId {
				found = true
				break
			}
		}
		if !found {
			n.KnownIds = append(n.KnownIds, feedbackId)
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
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("fail to dial: %v\n", err)
		return nil, nil, err
	}
	client := pb.NewVeriServiceClient(conn)
	return client, conn, nil
}
