package veriserviceserver

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/bgokden/veri/node"
)

func RunServer(configMap map[string]interface{}) {
	Health = true
	Ready = true

	services := configMap["services"].(string)
	log.Printf("Services: %v\n", services)
	port := configMap["port"].(int)
	// evictable := configMap["evictable"].(bool)
	// tls := configMap["tls"].(bool)
	// certFile := configMap["cert"].(string)
	// keyFile := configMap["key"].(string)
	// // memory := configMap["memory"].(uint64)
	// lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	// if err != nil {
	// 	log.Printf("failed to listen: %v", err)
	// 	return
	// }
	// var opts []grpc.ServerOption
	// if tls {
	// 	if certFile == "" {
	// 		certFile = testdata.Path("server1.pem")
	// 	}
	// 	if keyFile == "" {
	// 		keyFile = testdata.Path("server1.key")
	// 	}
	// 	creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
	// 	if err != nil {
	// 		log.Printf("Failed to generate credentials %v", err)
	// 		return
	// 	}
	// 	opts = []grpc.ServerOption{grpc.Creds(creds)}
	// }
	// grpcServer := grpc.NewServer(opts...)
	directory := configMap["directory"].(string)
	if len(directory) == 0 {
		var err error
		directory, err = ioutil.TempDir("", "node")
		if err != nil {
			log.Fatal(err)
		}
	}
	os.MkdirAll(directory, os.ModePerm)
	defer os.RemoveAll(directory)
	broadcast := configMap["broadcast"].(string)
	broadcastAddresses := make([]string, 0)
	if len(broadcast) > 0 {
		broadcastAddresses = append(broadcastAddresses, strings.Split(broadcast, ",")...)
	}
	for i, address := range broadcastAddresses {
		if !strings.Contains(address, ":") {
			broadcastAddresses[i] = fmt.Sprintf("%v:%v", address, port)
		}
	}

	serviceList := make([]string, 0)
	if len(services) > 0 {
		serviceList = append(serviceList, strings.Split(services, ",")...)
	}
	nodeConfig := &node.NodeConfig{
		Port:          uint32(port),
		Folder:        directory,
		AdvertisedIds: broadcastAddresses,
		ServiceList:   serviceList,
	}
	s := node.NewNode(nodeConfig)
	// pb.RegisterVeriServiceServer(grpcServer, s)
	go RestApi()
	go func() {
		sigint := make(chan os.Signal, 1)

		// interrupt signal sent from terminal
		signal.Notify(sigint, os.Interrupt)
		// sigterm signal sent from orchastrator
		signal.Notify(sigint, syscall.SIGTERM)

		<-sigint

		log.Printf("Closing services started.")
		// Cleap up here
		s.Close()
		os.Exit(0)
	}()
	log.Printf("Server started.")
	// grpcServer.Serve(lis)
	s.Listen()
}
