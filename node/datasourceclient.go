package node

import (
	"context"
	"log"
	"sync"
	"time"

	pb "github.com/bgokden/veri/veriservice"
	data "github.com/bgoken/veri/data"
	"google.golang.org/grpc"
)

func (p *Peer) GetDataSourceClient(name string) data.DataSource {
	return &DataSourceClient{
		Ids:  p.Ids,
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

func (dcs *DataSourceClient) StreamSearch(datum *data.Datum, scoredDatumStream chan<- *data.ScoredDatum, queryWaitGroup *sync.WaitGroup, config *data.SearchConfig) error {
	client, _, err := dcs.GetVeriServiceClient()
	if err != nil {
		return err
	}
	protoDatum := ConvertDataDatumToProtoDatum(datum)
	protoConfig := ConvertDataSearchConfigToProtoSearchConfig(config)
	searchRequest := &pb.searchRequest{}
	stream, err = client.StreamSearch(context.Background(), searchRequest)
	if err != nil {
		return err
	}
	for {
		protoScoredDatum, err := stream.Recv()
		if err != nil {
			log.Printf("Error: (%v)", err)
			break
		}
		scoredDatumStream <- ConvertProtoScoredDatumToDataScoredDatum(protoScoredDatum)
	}
	return err
}

func (dcs *DataSourceClient) StreamInsert(datumStream <-chan *data.InsertDatumWithConfig) error {
	return nil
}
func (dcs *DataSourceClient) Insert(datum *data.Datum, config *data.InsertConfig) error {
	client, _, err := dcs.GetVeriServiceClient()
	if err != nil {
		return err
	}
	request := &pb.InsertionRequest{
		Config: ConvertDataInsertConfigToProtoInsertConfig(config),
		Datum:  ConvertDataDatumToProtoDatum(datum),
	}
	_, err = client.Insert(context.Background(), request)
	return err
}
func (dcs *DataSourceClient) GetDataInfo() *data.DataInfo {
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
	return ConvertProtoDataInfoToDataInfo(dataInfo)
}
func (dcs *DataSourceClient) GetID() string {
	return dcs.Name
}

func ConvertDataInsertConfigToProtoInsertConfig(config *data.InsertConfig) *pb.InsertConfig {
	return &pb.InsertConfig{
		Ttl: uint64(config.TTL.Seconds()),
	}
}

func ConvertProtoInsertConfigToDataInsertConfig(config *pb.InsertConfig) *data.InsertConfig {
	return &data.InsertConfig{
		TTL: time.Duration(config.GetTtl()) * time.Second,
	}
}

func ConvertProtoDataInfoToDataInfo(dataInfo *pb.DataInfo) *data.DataInfo {
	return &data.DataInfo{
		Name:      dataInfo.Name,
		Timestamp: dataInfo.Timestamp,
		Version:   dataInfo.Version,
		Avg:       dataInfo.Avg,
		Hist:      dataInfo.Hist,
		N:         dataInfo.N,
	}
}

func ConvertDataInfoToPRotoDataInfo(dataInfo *data.DataInfo) *pb.DataInfo {
	return &pb.DataInfo{
		Name:      dataInfo.Name,
		Timestamp: dataInfo.Timestamp,
		Version:   dataInfo.Version,
		Avg:       dataInfo.Avg,
		Hist:      dataInfo.Hist,
		N:         dataInfo.N,
	}
}
