package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/magneticio/go-common/logging"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"

	"github.com/bgokden/veri/server"
	pb "github.com/bgokden/veri/veriservice"
)

var (
	tls        = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile   = flag.String("cert_file", "", "The TLS cert file")
	keyFile    = flag.String("key_file", "", "The TLS key file")
	jsonDBFile = flag.String("json_db_file", "testdata/route_guide_db.json", "A json file containing a list of features")
	port       = flag.Int("port", 10000, "The server port")
	services   = flag.String("services", "", "Comma separated list of services")
	evictable  = flag.Bool("evictable", true, "Data is evicted by time if it is true")
)

func main() {
	flag.Parse()
	logging.Init(os.Stdout, os.Stderr)
	logging.Verbose = true
	veriserviceserver.Health = true
	veriserviceserver.Ready = true

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		logging.Error("failed to listen: %v", err)
		return
	}
	var opts []grpc.ServerOption
	if *tls {
		if *certFile == "" {
			*certFile = testdata.Path("server1.pem")
		}
		if *keyFile == "" {
			*keyFile = testdata.Path("server1.key")
		}
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		if err != nil {
			logging.Error("Failed to generate credentials %v", err)
			return
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	grpcServer := grpc.NewServer(opts...)
	s := veriserviceserver.NewServer(*services, *evictable)
	pb.RegisterVeriServiceServer(grpcServer, s)
	go veriserviceserver.RestApi()
	logging.Info("Server started.")
	grpcServer.Serve(lis)
}
