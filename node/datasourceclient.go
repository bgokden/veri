package node

import (
	"context"
	"errors"
	"log"
	"sync"

	data "github.com/bgokden/veri/data"
	"github.com/bgokden/veri/util"
	pb "github.com/bgokden/veri/veriservice"
)

func GetDataSourceClient(p *pb.Peer, name string, idOfPeer string) data.DataSource {
	return &DataSourceClient{
		Ids:      []string{idOfPeer},
		Name:     name,
		ConnPool: util.NewConnectionPool(idOfPeer),
	}
}

type DataSourceClient struct {
	Ids      []string
	Name     string
	ConnPool *util.ConnectionPool
}

// func (dcs *DataSourceClient) GetVeriServiceClient() (pb.VeriServiceClient, *grpc.ClientConn, error) {
// 	// This can be a client pool
// 	address := dcs.Ids[0]
// 	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithTimeout(time.Duration(200)*time.Millisecond))
// 	if err != nil {
// 		log.Printf("fail to dial: %v\n", err)
// 		return nil, nil, err
// 	}
// 	client := pb.NewVeriServiceClient(conn)
// 	return client, conn, nil
// }

func (dcs *DataSourceClient) StreamSearch(datum *pb.Datum, scoredDatumStream chan<- *pb.ScoredDatum, queryWaitGroup *sync.WaitGroup, config *pb.SearchConfig) error {
	defer queryWaitGroup.Done()
	conn := dcs.ConnPool.Get()
	if conn == nil {
		return errors.New("Connection failure")
	}
	defer dcs.ConnPool.PutIfHealthy(conn)
	client := conn.Client
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
			// log.Printf("Error: (%v)", err)
			break
		}
		// log.Printf("Received Score: (%v)", protoScoredDatum.Score)
		scoredDatumStream <- protoScoredDatum
	}
	return err
}

func (dcs *DataSourceClient) Insert(datum *pb.Datum, config *pb.InsertConfig) error {
	conn := dcs.ConnPool.Get()
	if conn == nil {
		return errors.New("Connection failure")
	}
	defer dcs.ConnPool.PutIfHealthy(conn)
	client := conn.Client
	request := &pb.InsertionRequest{
		Config:   config,
		Datum:    datum,
		DataName: dcs.Name,
	}
	_, err := client.Insert(context.Background(), request)
	return err
}

func (dcs *DataSourceClient) GetDataInfo() *pb.DataInfo {
	conn := dcs.ConnPool.Get()
	if conn == nil {
		log.Printf("Connection failure\n")
		return nil
	}
	defer dcs.ConnPool.PutIfHealthy(conn)
	client := conn.Client
	request := &pb.GetDataRequest{
		Name: dcs.Name,
	}
	dataInfo, err := client.GetDataInfo(context.Background(), request)
	if err != nil {
		log.Printf("GetDataInfo Error: %v\n", err.Error())
		return nil
	}
	return dataInfo
}

func (dcs *DataSourceClient) GetID() string {
	return SerializeStringArray(dcs.Ids)
}
