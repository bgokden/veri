package veriserviceserver

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"

	"github.com/bgokden/veri/node"
	pb "github.com/bgokden/veri/veriservice"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
)

func RunServer(configMap map[string]interface{}) {
	Health = true
	Ready = true

	services := configMap["services"].(string)
	log.Printf("Services: %v\n", services)
	port := configMap["port"].(int)
	// evictable := configMap["evictable"].(bool)
	tls := configMap["tls"].(bool)
	certFile := configMap["cert"].(string)
	keyFile := configMap["key"].(string)
	// memory := configMap["memory"].(uint64)
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Printf("failed to listen: %v", err)
		return
	}
	var opts []grpc.ServerOption
	if tls {
		if certFile == "" {
			certFile = testdata.Path("server1.pem")
		}
		if keyFile == "" {
			keyFile = testdata.Path("server1.key")
		}
		creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
		if err != nil {
			log.Printf("Failed to generate credentials %v", err)
			return
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	grpcServer := grpc.NewServer(opts...)
	dir0, err := ioutil.TempDir("tmp", "node")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir0)
	broadcastAddresses := []string{fmt.Sprintf("localhost:%d", port)}
	nodeConfig := &node.NodeConfig{
		Port:          uint32(port),
		Folder:        dir0,
		AdvertisedIds: broadcastAddresses,
		ServiceList:   []string{services},
	}
	s := node.NewNode(nodeConfig) //NewServer(services, evictable)
	pb.RegisterVeriServiceServer(grpcServer, s)
	go RestApi()
	log.Printf("Server started.")
	grpcServer.Serve(lis)
}
