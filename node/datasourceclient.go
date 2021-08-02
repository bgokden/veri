package node

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	data "github.com/bgokden/veri/data"
	"github.com/bgokden/veri/util"
	pb "github.com/bgokden/veri/veriservice"
)

func GetDataSourceClient(p *pb.Peer, name string, idOfPeer string, connectionCache *util.ConnectionCache) data.DataSource {
	return &DataSourceClient{
		Ids:             []string{idOfPeer},
		Name:            name,
		IdOfPeer:        idOfPeer,
		ConnectionCache: connectionCache,
	}
}

type DataSourceClient struct {
	Ids             []string
	Name            string
	IdOfPeer        string
	ConnectionCache *util.ConnectionCache
}

func (dcs *DataSourceClient) StreamSearch(datum *pb.Datum, scoredDatumStream chan<- *pb.ScoredDatum, queryWaitGroup *sync.WaitGroup, config *pb.SearchConfig) error {
	defer queryWaitGroup.Done()
	clientCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn := dcs.ConnectionCache.Get(clientCtx, dcs.IdOfPeer)
	if conn == nil {
		return errors.New("Connection failure")
	}
	defer conn.Close()
	client := pb.NewVeriServiceClient(conn)
	searchRequest := &pb.SearchRequest{
		Datum:  []*pb.Datum{datum},
		Config: config,
	}
	stream, err := client.SearchStream(clientCtx, searchRequest)
	if err != nil {
		return err
	}
	for {
		protoScoredDatum, err := stream.Recv()
		if err != nil {
			// log.Printf("Error: (%v)", err)
			break
		}
		// log.Printf("Received From:  %v for Score: %v Label: %v", dcs.Ids, protoScoredDatum.Score, string(protoScoredDatum.GetDatum().GetValue().GetLabel()))
		scoredDatumStream <- protoScoredDatum
	}
	return err
}

func (dcs *DataSourceClient) Insert(datum *pb.Datum, config *pb.InsertConfig) error {
	clientCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn := dcs.ConnectionCache.Get(clientCtx, dcs.IdOfPeer)
	if conn == nil {
		return errors.New("Connection failure")
	}
	defer conn.Close()
	client := pb.NewVeriServiceClient(conn)
	request := &pb.InsertionRequest{
		Config:   config,
		Datum:    datum,
		DataName: dcs.Name,
	}
	_, err := client.Insert(clientCtx, request)
	return err
}

func (dcs *DataSourceClient) GetDataInfo() *pb.DataInfo {
	clientCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn := dcs.ConnectionCache.Get(clientCtx, dcs.IdOfPeer)
	if conn == nil {
		log.Printf("Connection failure\n")
		return nil
	}
	defer conn.Close()
	client := pb.NewVeriServiceClient(conn)
	request := &pb.GetDataRequest{
		Name: dcs.Name,
	}
	dataInfo, err := client.GetDataInfo(clientCtx, request)
	if err != nil {
		log.Printf("GetDataInfo Error: %v\n", err.Error())
		return nil
	}
	return dataInfo
}

func (dcs *DataSourceClient) GetID() string {
	return SerializeStringArray(dcs.Ids)
}
