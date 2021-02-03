package node

import (
	"context"
	"log"
	"sync"

	data "github.com/bgokden/veri/data"
	pb "github.com/bgokden/veri/veriservice"
	"google.golang.org/grpc"
)

func GetDataSourceClient(p *pb.Peer, name string, idOfPeer string) data.DataSource {
	return &DataSourceClient{
		Ids:  []string{idOfPeer},
		Name: name,
	}
}

type DataSourceClient struct {
	Ids  []string
	Name string
}

func (dcs *DataSourceClient) GetVeriServiceClient() (pb.VeriServiceClient, *grpc.ClientConn, error) {
	// This can be a client pool
	address := dcs.Ids[0]
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("fail to dial: %v\n", err)
		return nil, nil, err
	}
	client := pb.NewVeriServiceClient(conn)
	return client, conn, nil
}

func (dcs *DataSourceClient) StreamSearch(datum *pb.Datum, scoredDatumStream chan<- *pb.ScoredDatum, queryWaitGroup *sync.WaitGroup, config *pb.SearchConfig) error {
	defer queryWaitGroup.Done()
	client, _, err := dcs.GetVeriServiceClient()
	if err != nil {
		return err
	}
	searchRequest := &pb.SearchRequest{
		Datum:  []*pb.Datum{datum},
		Config: config,
	}
	stream, err := client.SearchStream(context.Background(), searchRequest)
	if err != nil {
		return err
	}
	for {
		protoScoredDatum, err := stream.Recv()
		if err != nil {
			log.Printf("Error: (%v)", err)
			break
		}
		log.Printf("Received Score: (%v)", protoScoredDatum.Score)
		scoredDatumStream <- protoScoredDatum
	}
	return err
}

func (dcs *DataSourceClient) Insert(datum *pb.Datum, config *pb.InsertConfig) error {
	client, _, err := dcs.GetVeriServiceClient()
	if err != nil {
		return err
	}
	request := &pb.InsertionRequest{
		Config: config,
		Datum:  datum,
	}
	_, err = client.Insert(context.Background(), request)
	return err
}

func (dcs *DataSourceClient) GetDataInfo() *pb.DataInfo {
	client, _, err := dcs.GetVeriServiceClient()
	if err != nil {
		return nil
	}
	request := &pb.GetDataRequest{
		Name: dcs.Name,
	}
	dataInfo, err := client.GetDataInfo(context.Background(), request)
	if err != nil {
		return nil
	}
	return dataInfo
}

func (dcs *DataSourceClient) GetID() string {
	return dcs.Name
}
