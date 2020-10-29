package node

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	pb "github.com/bgokden/veri/veriservice"
	data "github.com/bgoken/veri/data"
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
	config := ConvertProtoInsertConfigToDataInsertConfig(insertionRequest.GetConfig())
	datum := ConvertProtoDatumToDataDatum(insertionRequest.GetDatum())
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
	peer := ConvertJoinRequestToPeer(joinRequest)
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
	datumStreamAll := make(chan *data.Datum, 100)
	err = dt.StreamAll(datumStreamAll)
	if err != nil {
		return err
	}
	for datum := range datumStreamAll {
		stream.Send(ConvertDataDatumToProtoDatum(datum))
	}
	return nil
}

func (n *Node) GetDataInfo(ctx context.Context, getDataRequest *pb.GetDataRequest) (*pb.DataInfo, error) {
	name := getDataRequest.Name
	data, err := n.Dataset.Get(name)
	if err != nil {
		return nil, err
	}
	if data != nil {
		return ConvertDataInfoToPRotoDataInfo(data.GetDataInfo()), nil
	}
	return nil, nil
}

func (n *Node) SearchStream(searchRequest *pb.SearchRequest, stream pb.VeriService_SearchStreamServer) error {
	config := searchRequest.GetConfig()
	aData, err := n.Dataset.Get(config.GetDataName())
	if err != nil {
		return err
	}
	datum := ConvertProtoDatumToDataDatum(searchRequest.GetDatum())
	searchConfig := ConvertProtoSearchConfigToDataSearchConfig(searchRequest.GetConfig())
	scoredDatumStream := make(chan *data.ScoredDatum, 100)
	defer close(scoredDatumStream)
	err = aData.SuperSearch(datum, scoredDatumStream, searchConfig)
	if err != nil {
		return err
	}
	for e := range scoredDatumStream {
		log.Printf("label: %v score: %v\n", string(e.Datum.Value.Label), e.Score)
		scoredDatum := &pb.ScoredDatum{
			Datum: ConvertDataDatumToProtoDatum(e.Datum),
			Score: e.Score,
		}
		stream.Send(scoredDatum)
	}
	return nil
}

func (n *Node) InsertStream(stream pb.VeriService_InsertStreamServer) error {
	return nil
}

func ConvertProtoDatumToDataDatum(datum *pb.Datum) *data.Datum {
	return data.NewDatum(
		datum.GetFeature(),
		datum.GetDim1(),
		datum.GetDim2(),
		datum.GetSize1(),
		datum.GetSize2(),
		datum.GetGroupLabel(),
		datum.GetLabel(),
		datum.GetVersion(),
	)
}

func ConvertDataDatumToProtoDatum(datum *data.Datum) *pb.Datum {
	return &pb.Datum{
		Feature:    datum.Key.Feature,
		Version:    datum.Value.Version,
		Label:      datum.Value.Label,
		GroupLabel: datum.Key.GroupLabel,
		Size1:      datum.Key.Size1,
		Size2:      datum.Key.Size2,
		Dim1:       datum.Key.Dim1,
		Dim2:       datum.Key.Dim2,
	}
}

func ConvertProtoSearchConfigToDataSearchConfig(config *pb.SearchConfig) *data.SearchConfig {
	searchConfig := data.DefaultSearchConfig()

	searchConfig.ScoreFuncName = config.GetScoreFuncName()
	searchConfig.ScoreFunc = nil
	searchConfig.HigherIsBetter = config.GetHigherIsBetter()
	searchConfig.Limit = config.GetLimit()
	searchConfig.Duration = time.Duration(config.GetTimeout())
	return searchConfig
}

func ConvertJoinRequestToPeer(joinRequest *pb.JoinRequest) *Peer {
	return ConvertProtoPeerToNodePeer(joinRequest.GetPeer())

}

func ConvertProtoPeerToNodePeer(pbPeer *pb.Peer) *Peer {
	nodePeer := &Peer{
		Ids:         pbPeer.GetAddresses(),
		ServiceList: pbPeer.GetServices(),
		PeerList:    make([]Peer, 0),
		DataList:    make([]data.DataConfig, 0),
	}
	for _, pbPeerElement := range pbPeer.GetPeers() {
		nodePeer.PeerList = append(nodePeer.PeerList, *ConvertProtoPeerToNodePeer(pbPeerElement))
	}
	for _, pbConfigElement := range pbPeer.GetData() {
		nodePeer.DataList = append(nodePeer.DataList, *ConvertProtoDataConfigToDataConfig(pbConfigElement))
	}
	return nodePeer

}

func ConvertProtoDataConfigToDataConfig(config *pb.DataConfig) *data.DataConfig {
	return &data.DataConfig{
		Name:    config.Name,
		Version: config.Version,
		TargetN: config.TargetN,
	}
}

func ConvertDataConfigToProtoDataConfig(config *data.DataConfig) *pb.DataConfig {
	return &pb.DataConfig{
		Name:    config.Name,
		Version: config.Version,
		TargetN: config.TargetN,
	}
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
		Peer: ConvertNodePeerToProtoPeer(peerInfo),
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

func ConvertNodePeerToProtoPeer(p *Peer) *pb.Peer {
	pbPeer := &pb.Peer{
		Addresses: p.Ids,
		Version:   p.Version,
		Timestamp: p.Timestamp,
		Data:      make([]*pb.DataConfig, 0),
		Services:  p.ServiceList,
		Peers:     make([]*pb.Peer, 0),
	}
	for _, config := range p.DataList {
		pbPeer.Data = append(pbPeer.Data, ConvertDataConfigToProtoDataConfig(&config))
	}
	for _, nodePeer := range p.PeerList {
		pbPeer.Peers = append(pbPeer.Peers, ConvertNodePeerToProtoPeer(&nodePeer))
	}
	return pbPeer
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
