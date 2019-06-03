package veriserviceserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bgokden/veri/data"
	"github.com/bgokden/veri/models"
	pb "github.com/bgokden/veri/veriservice"
	"github.com/goburrow/cache"
	"github.com/magneticio/go-common/logging"
	"github.com/segmentio/ksuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	grpcPeer "google.golang.org/grpc/peer"
	"google.golang.org/grpc/testdata"
)

type VeriServiceServer struct {
	k            int64
	address      string
	version      string
	timestamp    int64
	state        int
	maxMemoryMiB uint64
	services     sync.Map
	peers        sync.Map
	knnQueryId   cache.Cache
	cache        cache.Cache
	clients      sync.Map
	dt           *data.Data
}

var port int = 10000

// This is set in compile time for optimization
const k = 1024 // 1024

// 0 => euclidean distance
// 1 => consine distance
const distance_mode = 0

func getCurrentTime() int64 {
	return time.Now().Unix()
}

func (s *VeriServiceServer) GetLocalData(rect *pb.GetLocalDataRequest, stream pb.VeriService_GetLocalDataServer) error {
	return s.dt.GetAll(stream)
}

func (s *VeriServiceServer) GetKnnFromPeer(in *pb.KnnRequest, peer *models.Peer, featuresChannel chan<- pb.Feature) {
	log.Printf("GetKnnFromPeer %s", peer.Address)
	client, err0 := s.get_client(peer.Address)
	if err0 == nil {
		grpc_client := client.Client
		resp, err := (*grpc_client).GetKnnStream(context.Background(), in)
		if err != nil {
			log.Printf("There is an error: %v", err)
			// conn.Close()
			go s.refresh_client(peer.Address)
			return
		}
		// if resp.Success {
		// log.Printf("A new Response has been received with id: %s", resp.Id)
		for {
			feature, err := resp.Recv()
			if err != nil {
				log.Printf("Error: (%v)", err)
				break
			}
			log.Printf("New Feature from Peer (%s) : %v", peer.Address, feature.GetLabel())
			featuresChannel <- *(feature)
		}
		// conn.Close()
	}
}

func (s *VeriServiceServer) GetKnnFromPeers(in *pb.KnnRequest, featuresChannel chan<- pb.Feature) {
	timeout := int64(float64(in.GetTimeout()) * 0.9)
	request := &pb.KnnRequest{
		Feature:   in.GetFeature(),
		Id:        in.GetId(),
		K:         in.GetK(),
		Timestamp: in.GetTimestamp(),
		Timeout:   timeout,
	}
	logging.Info("GetKnnFromPeers")
	// TODO: get recommended peers instead of all peers
	s.peers.Range(func(key, value interface{}) bool {
		go func() {
			peerAddress := key.(string)
			logging.Info("Querying Peer %v\n", peerAddress)
			if len(peerAddress) > 0 && peerAddress != s.address {
				peerValue := value.(models.Peer)
				s.GetKnnFromPeer(request, &peerValue, featuresChannel)
			}
		}()
		return true
	})
}

func (s *VeriServiceServer) GetKnnFromLocal(in *pb.KnnRequest, featuresChannel chan<- pb.Feature) {
	log.Printf("GetKnnFromLocal")
	point := data.NewEuclideanPointArr(in.GetFeature())
	ans, err := s.dt.GetKnn(int64(in.GetK()), point)
	if err == nil {
		for i := 0; i < len(ans); i++ {
			// log.Printf("New Feature from Local before")
			feature := data.NewFeatureFromEuclideanPoint(ans[i])
			// log.Printf("New Feature from Local: %v after", feature.GetLabel())
			featuresChannel <- *feature
		}
	} else {
		log.Printf("Error in GetKnn: %v", err.Error())
	}
}

// Do a distributed Knn search
func (s *VeriServiceServer) GetKnn(ctx context.Context, in *pb.KnnRequest) (*pb.KnnResponse, error) {
	request := *in
	d := int64(len(request.GetFeature()))
	var featureHash [k]float64
	copy(featureHash[:d], request.GetFeature()[:])
	if len(in.GetId()) == 0 {
		request.Id = ksuid.New().String()
		s.knnQueryId.Put(request.Id, true)
	} else {
		_, loaded := s.knnQueryId.GetIfPresent(request.GetId())
		if loaded {
			cachedResult, isCached := s.cache.GetIfPresent(featureHash)
			if isCached {
				log.Printf("Return cached result for id %v", request.GetId())
				return cachedResult.(*pb.KnnResponse), nil
			} else {
				log.Printf("Return un-cached result for id %v since it is already processed.", request.GetId())
				return &pb.KnnResponse{Id: in.Id, Features: nil}, nil
			}
		} else {
			s.knnQueryId.Put(request.GetId(), getCurrentTime())
		}
	}
	featuresChannel := make(chan pb.Feature, in.GetK())
	go s.GetKnnFromPeers(&request, featuresChannel)
	go s.GetKnnFromLocal(&request, featuresChannel)
	// time.Sleep(1 * time.Second)
	// close(featuresChannel)
	responseFeatures := make([]*pb.Feature, 0)
	dataAvailable := true
	timeLimit := time.After(time.Duration(in.GetTimeout()) * time.Millisecond)
	// reduceMap := make(map[data.EuclideanPointKey]data.EuclideanPointValue)
	reduceData := data.NewTempData()
	for dataAvailable {
		select {
		case feature := <-featuresChannel:
			key, value := data.FeatureToEuclideanPointKeyValue(&feature)
			// reduceMap[*key] = *value
			reduceData.Insert(*key, *value)
		case <-timeLimit:
			log.Printf("timeout")
			dataAvailable = false
			break
		}
	}
	point := data.NewEuclideanPointArr(in.Feature)
	reduceData.Process(true)
	ans, err := reduceData.GetKnn(int64(in.K), point)
	if err != nil {
		log.Printf("Error in Knn: %v", err.Error())
		return &pb.KnnResponse{Id: request.GetId(), Features: responseFeatures}, err
	}
	for i := 0; i < len(ans); i++ {
		featureJson := data.NewFeatureFromPoint(ans[i])
		// log.Printf("New Feature (Get Knn): %v", ans[i].GetLabel())
		responseFeatures = append(responseFeatures, featureJson)
	}
	s.knnQueryId.Put(request.GetId(), true)
	s.cache.Put(featureHash, &pb.KnnResponse{Id: request.GetId(), Features: responseFeatures})
	return &pb.KnnResponse{Id: request.GetId(), Features: responseFeatures}, nil
}

func (s *VeriServiceServer) GetKnnStream(in *pb.KnnRequest, stream pb.VeriService_GetKnnStreamServer) error {
	request := *in
	d := int64(len(request.GetFeature()))
	var featureHash [k]float64
	copy(featureHash[:d], request.GetFeature()[:])
	if len(in.GetId()) == 0 {
		request.Id = ksuid.New().String()
		s.knnQueryId.Put(request.Id, true)
	} else {
		_, loaded := s.knnQueryId.GetIfPresent(request.GetId())
		if loaded {
			cachedResult, isCached := s.cache.GetIfPresent(featureHash)
			if isCached {
				log.Printf("Return cached result for id %v", request.GetId())
				result := cachedResult.(*pb.KnnResponse).GetFeatures()
				for _, e := range result {
					stream.Send(e)
				}
				return nil
			} else {
				log.Printf("Return un-cached result for id %v since it is already processed.", request.GetId())
				return nil
			}
		} else {
			s.knnQueryId.Put(request.GetId(), getCurrentTime())
		}
	}
	featuresChannel := make(chan pb.Feature, in.GetK())
	go s.GetKnnFromPeers(&request, featuresChannel)
	go s.GetKnnFromLocal(&request, featuresChannel)
	// time.Sleep(1 * time.Second)
	// close(featuresChannel)
	responseFeatures := make([]*pb.Feature, 0)
	dataAvailable := true
	timeLimit := time.After(time.Duration(in.GetTimeout()) * time.Millisecond)
	// reduceMap := make(map[data.EuclideanPointKey]data.EuclideanPointValue)
	reduceData := data.NewTempData()
	for dataAvailable {
		select {
		case feature := <-featuresChannel:
			key, value := data.FeatureToEuclideanPointKeyValue(&feature)
			// reduceMap[*key] = *value
			reduceData.Insert(*key, *value)
		case <-timeLimit:
			log.Printf("timeout")
			dataAvailable = false
			break
		}
	}
	point := data.NewEuclideanPointArr(in.Feature)
	reduceData.Process(true)
	ans, err := reduceData.GetKnn(int64(in.K), point)
	if err != nil {
		log.Printf("Error in Knn: %v", err.Error())
		return err
	}
	for i := 0; i < len(ans); i++ {
		feature := data.NewFeatureFromPoint(ans[i])
		// log.Printf("New Feature (Get Knn): %v", ans[i].GetLabel())
		stream.Send(feature)
		responseFeatures = append(responseFeatures, feature)
	}
	s.knnQueryId.Put(request.GetId(), true)
	s.cache.Put(featureHash, &pb.KnnResponse{Id: request.GetId(), Features: responseFeatures})
	return nil
}

func (s *VeriServiceServer) Insert(ctx context.Context, in *pb.InsertionRequest) (*pb.InsertionResponse, error) {
	if s.state > 2 {
		return &pb.InsertionResponse{Code: 1}, nil
	}
	key, value := data.InsertionRequestToEuclideanPointKeyValue(in)
	s.dt.Insert(*key, *value)
	return &pb.InsertionResponse{Code: 0}, nil
}

func (s *VeriServiceServer) InsertStream(stream pb.VeriService_InsertStreamServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Fatalf("Failed to receive a note : %v", err)
		}
		key, value := data.FeatureToEuclideanPointKeyValue(in)
		s.dt.Insert(*key, *value)

		if s.state > 2 {
			stream.Send(&pb.InsertionResponse{Code: 1})
			return nil
		} else {
			stream.Send(&pb.InsertionResponse{Code: 0})
		}
	}
	// return nil
}

func (s *VeriServiceServer) Join(ctx context.Context, in *pb.JoinRequest) (*pb.JoinResponse, error) {
	// logging.Info("Join request received %v\n", *in)
	p, ok := grpcPeer.FromContext(ctx)
	if !ok {
		logging.Error("Peer can not be get from context %v\n", p)
		return nil, errors.New("Peer can not be get from context")
	}
	address := strings.Split(p.Addr.String(), ":")[0] + ":" + strconv.FormatInt(int64(in.GetPort()), 10)
	// log.Printf("Peer with Addr: %s called Join", address)
	peerStruct := models.Peer{
		Address:   address,
		Avg:       in.GetAvg(),
		Version:   in.GetVersion(),
		Hist:      in.GetHist(),
		N:         in.GetN(),
		Timestamp: in.GetTimestamp(),
	}
	s.peers.Store(address, peerStruct)
	return &pb.JoinResponse{Address: address}, nil
}

func (s *VeriServiceServer) ExchangeServices(ctx context.Context, in *pb.ServiceMessage) (*pb.ServiceMessage, error) {
	inputServiceList := in.GetServices()
	for i := 0; i < len(inputServiceList); i++ {
		s.services.Store(inputServiceList[i], true)
	}
	outputServiceList := make([]string, 0)
	s.services.Range(func(key, value interface{}) bool {
		serviceName := key.(string)
		outputServiceList = append(outputServiceList, serviceName)
		return true
	})
	return &pb.ServiceMessage{Services: outputServiceList}, nil
}

func (s *VeriServiceServer) ExchangePeers(ctx context.Context, in *pb.PeerMessage) (*pb.PeerMessage, error) {
	inputPeerList := in.GetPeers()
	for i := 0; i < len(inputPeerList); i++ {
		insert := true
		temp, ok := s.peers.Load(inputPeerList[i].GetAddress())
		if ok {
			peerOld := temp.(models.Peer)
			if peerOld.Timestamp > inputPeerList[i].GetTimestamp() || inputPeerList[i].GetTimestamp()+300 < getCurrentTime() {
				insert = false
			}
		}
		if insert {
			peer := models.Peer{
				Address:   inputPeerList[i].GetAddress(),
				Version:   inputPeerList[i].GetVersion(),
				Avg:       inputPeerList[i].GetAvg(),
				Hist:      inputPeerList[i].GetHist(),
				N:         inputPeerList[i].GetN(),
				Timestamp: inputPeerList[i].GetTimestamp(),
			}
			s.peers.Store(inputPeerList[i].GetAddress(), peer)
		}
	}
	outputPeerList := make([]*pb.Peer, 0)
	s.peers.Range(func(key, value interface{}) bool {
		// address := key.(string)
		peer := value.(models.Peer)
		if peer.Timestamp+300 > getCurrentTime() {
			peerProto := &pb.Peer{
				Address:   peer.Address,
				Version:   peer.Version,
				Avg:       peer.Avg,
				Hist:      peer.Hist,
				N:         peer.N,
				Timestamp: peer.Timestamp,
			}
			outputPeerList = append(outputPeerList, peerProto)
		}
		return true
	})
	return &pb.PeerMessage{Peers: outputPeerList}, nil
}

func (s *VeriServiceServer) getClient(address string) (*pb.VeriServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		logging.Error("fail to dial: %v\n", err)
		return nil, nil, err
	}
	client := pb.NewVeriServiceClient(conn)
	return &client, conn, nil
}

func (s *VeriServiceServer) new_client(address string) (*models.Client, error) {
	client, conn, err := s.getClient(address)
	if err != nil {
		logging.Error("fail to create a client: %v\n", err)
		return nil, err
	}
	logging.Info("Client created with address: %v\n", address)
	return &models.Client{
		Address: address,
		Client:  client,
		Conn:    conn,
	}, nil
}

/*
There may be some concurrency problems where unclosed connections can occur
*/
func (s *VeriServiceServer) get_client(address string) (models.Client, error) {
	client, ok := s.clients.Load(address)
	if ok {
		logging.Info("Using existing client for %v\n", address)
		newClientPointer, castOk := client.(*models.Client)
		if castOk {
			return *newClientPointer, nil
		} else {
			return models.Client{}, errors.New("Client can not be created")
		}
	} else {
		newClientPointer, err := s.new_client(address)
		if err != nil {
			logging.Error("Client creation failed: %v\n", err)
			return models.Client{}, err
		} else {
			logging.Info("Using new client for %v\n", address)
			s.clients.Store(address, newClientPointer)
			return *newClientPointer, nil
		}
	}
	// return models.Client{}, errors.New("Can not initilize client")
}

func (s *VeriServiceServer) refresh_client(address string) {
	log.Printf("Renewing client with address %v", address)
	new_client, err := s.new_client(address)
	if err != nil {
		log.Printf("fail to get a client: %v", err) // this is probably really bad
	} else {
		s.clients.Store(address, new_client)
	}
}

func (s *VeriServiceServer) callJoin(client *pb.VeriServiceClient) {
	stats := s.dt.GetStats()
	request := &pb.JoinRequest{
		Address:   s.address,
		Avg:       stats.Avg,
		Port:      int32(port),
		Version:   s.version,
		Hist:      stats.Hist,
		N:         stats.N,
		Timestamp: s.timestamp,
	}
	// logging.Info("Call Join Request %v", *request)
	resp, err := (*client).Join(context.Background(), request)
	if err != nil {
		logging.Error("(Call Join) There is an error %v", err)
		return
	}
	if s.address != resp.GetAddress() {
		s.address = resp.GetAddress()
	}
}

func (s *VeriServiceServer) callExchangeServices(client *pb.VeriServiceClient) {
	outputServiceList := make([]string, 0)
	s.services.Range(func(key, value interface{}) bool {
		serviceName := key.(string)
		outputServiceList = append(outputServiceList, serviceName)
		return true
	})
	request := &pb.ServiceMessage{
		Services: outputServiceList,
	}
	resp, err := (*s).ExchangeServices(context.Background(), request)
	if err != nil {
		log.Printf("(callExchangeServices) There is an error %v", err)
		return
	}
	inputServiceList := resp.GetServices()
	for i := 0; i < len(inputServiceList); i++ {
		s.services.Store(inputServiceList[i], true)
	}
	// log.Printf("Services exhanged")
}

func (s *VeriServiceServer) callExchangeData(client *pb.VeriServiceClient, peer *models.Peer) {
	// need to check avg and hist differences ...
	// chose datum
	stats := s.dt.GetStats()
	log.Printf("For peer: %v s.n: %d peer.N: %d peer timestamp: %v currentTime: %v", peer.Address, stats.N, peer.N, peer.Timestamp, getCurrentTime())
	if peer.Timestamp+360 < getCurrentTime() {
		log.Printf("Peer data is too old, maybe peer is dead: %s, peer timestamp: %d, current time: %d", peer.Address, peer.Timestamp, getCurrentTime())
		// Maybe remove the peer here
		s.peers.Delete(peer.Address)
		s.clients.Delete(peer.Address) // maybe try closing before delete
		return
	}
	if peer.Timestamp+30 < getCurrentTime() && s.state == 0 {
		// log.Printf("Peer data is too old: %s", peer.Address)
		// limit = 1 // no change can be risky
		return
	}
	if stats.N < peer.N {
		// log.Printf("Other peer should initiate exchange data %s", peer.Address)
		return
	}
	distanceAvg := data.VectorDistance(stats.Avg, peer.Avg)
	distanceHist := data.VectorDistance(stats.Hist, peer.Hist)
	// log.Printf("%s => distanceAvg %f, distanceHist: %f", peer.Address, distanceAvg, distanceHist)
	limit := int(((stats.N - peer.N) / 10) % 1000)
	nRatio := 0.0
	if peer.N != 0 {
		nRatio = float64(stats.N) / float64(peer.N)
	}
	if 0.99 < nRatio && nRatio < 1.01 && distanceAvg < 0.0005 && distanceHist < 0.0005 && s.state == 0 {
		// log.Printf("Decrease number of changes to 1 since stats are close enough %s", peer.Address)
		limit = 1 // no change can be risky
	}
	points := s.dt.GetRandomPoints(limit)

	for _, point := range points {
		request := data.NewInsertionRequestFromPoint(point)
		resp, err := (*s).Insert(context.Background(), request)
		if err != nil {
			log.Printf("There is an error: %v", err)
		} else {
			// log.Printf("A new Response has been received for %d. with code: %d", i, resp.GetCode())
			if resp.GetCode() == 0 && s.state > 0 && rand.Float64() < (0.3*float64(s.state)) {
				key := data.NewEuclideanPointKeyFromPoint(point)
				s.dt.Delete(*key)
			}
		}
	}
}

func (s *VeriServiceServer) callExchangePeers(client *pb.VeriServiceClient) {
	// log.Printf("callExchangePeers")
	outputPeerList := make([]*pb.Peer, 0)
	s.peers.Range(func(key, value interface{}) bool {
		// address := key.(string)
		peer := value.(models.Peer)
		peerProto := &pb.Peer{
			Address:   peer.Address,
			Version:   peer.Version,
			Avg:       peer.Avg,
			Hist:      peer.Hist,
			N:         peer.N,
			Timestamp: peer.Timestamp,
		}
		outputPeerList = append(outputPeerList, peerProto)
		return true
	})
	request := &pb.PeerMessage{
		Peers: outputPeerList,
	}
	resp, err := (*s).ExchangePeers(context.Background(), request)
	if err != nil {
		log.Printf("(callExchangePeers) There is an error %v", err)
		return
	}
	inputPeerList := resp.GetPeers()
	for i := 0; i < len(inputPeerList); i++ {
		insert := true
		temp, ok := s.peers.Load(inputPeerList[i].GetAddress())
		if ok {
			peerOld := temp.(models.Peer)
			if peerOld.Timestamp > inputPeerList[i].GetTimestamp() {
				insert = false
			}
		}
		if insert && s.address != inputPeerList[i].GetAddress() {
			peer := models.Peer{
				Address:   inputPeerList[i].GetAddress(),
				Version:   inputPeerList[i].GetVersion(),
				Avg:       inputPeerList[i].GetAvg(),
				Hist:      inputPeerList[i].GetHist(),
				N:         inputPeerList[i].GetN(),
				Timestamp: inputPeerList[i].GetTimestamp(),
			}
			s.peers.Store(inputPeerList[i].GetAddress(), peer)
		}
	}
	// log.Printf("Peers exhanged")
}

func (s *VeriServiceServer) SyncJoin() {
	// log.Printf("Sync Join")
	s.services.Range(func(key, value interface{}) bool {
		serviceName := key.(string)
		logging.Info("Service %s", serviceName)
		if len(serviceName) > 0 {
			client, err := s.get_client(serviceName)
			if err == nil {
				grpcClient := client.Client
				s.callJoin(grpcClient)
			} else {
				logging.Error("SyncJoin Error: %v\n", err)
				go s.refresh_client(serviceName)
			}
			// conn.Close()
		}
		return true
	})
	// log.Printf("Service loop Ended")
	s.peers.Range(func(key, value interface{}) bool {
		peerAddress := key.(string)
		// log.Printf("Peer %s", peerAddress)
		if len(peerAddress) > 0 && peerAddress != s.address {
			peerValue := value.(models.Peer)
			client, err := s.get_client(peerAddress)
			if err == nil {
				grpcClient := client.Client
				s.callJoin(grpcClient)
				s.callExchangeServices(grpcClient)
				s.callExchangePeers(grpcClient)
				s.callExchangeData(grpcClient, &peerValue)
			} else {
				logging.Error("SyncJoin Error: %v\n", err)
				go s.refresh_client(peerAddress)
			}
			// conn.Close()
		}
		return true
	})
	// log.Printf("Peer loop Ended")
}

var evictable bool

func (s *VeriServiceServer) isEvictable() bool {
	if s.state >= 2 && evictable {
		return true
	}
	return false
}

func NewServer(services string, evict bool) *VeriServiceServer {
	s := &VeriServiceServer{}
	evictable = evict
	s.dt = data.NewData()
	log.Printf("services %s", services)
	serviceList := strings.Split(services, ",")
	for _, service := range serviceList {
		if len(service) > 0 {
			s.services.Store(service, true)
		}
	}
	s.maxMemoryMiB = 1024
	s.timestamp = getCurrentTime()
	load := func(k cache.Key) (cache.Value, error) {
		return fmt.Sprintf("%d", k), nil
	}
	s.cache = cache.NewLoadingCache(load,
		cache.WithMaximumSize(1000),
		cache.WithExpireAfterAccess(10*time.Second),
		cache.WithRefreshAfterWrite(60*time.Second),
	)
	s.knnQueryId = cache.NewLoadingCache(load,
		cache.WithMaximumSize(1000),
		cache.WithExpireAfterAccess(10*time.Second),
		cache.WithRefreshAfterWrite(60*time.Second),
	)
	go s.Check()
	return s
}

func (s *VeriServiceServer) Check() {
	nextSyncJoinTime := getCurrentTime()
	for {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		// log.Printf("Alloc = %v MiB", bToMb(m.Alloc))
		// log.Printf("TotalAlloc = %v MiB", bToMb(m.TotalAlloc))
		// log.Printf("Sys = %v MiB", bToMb(m.Sys))
		// log.Printf("NumGC = %v\n", m.NumGC)
		currentMemory := float64(bToMb(m.Alloc))
		maxMemory := float64(s.maxMemoryMiB)
		if currentMemory < maxMemory*0.5 {
			s.state = 0 // Accept insert, don't delete while sending data
		} else if currentMemory < maxMemory*0.75 {
			s.state = 1 // Accept insert, delete while sending data
		} else if currentMemory < maxMemory*0.85 {
			s.state = 2 // Accept insert, delete while sending data and evict data
		} else {
			s.state = 3 // Don't accept insert, delete while sending data
		}
		// log.Printf("Current Memory = %f MiB => current State %d", currentMemory, s.state)
		// millisecondToSleep := int64(((s.latestNumberOfInserts + 100) % 1000) * 10)
		// log.Printf("millisecondToSleep: %d, len %d", millisecondToSleep, s.n)
		// time.Sleep(time.Duration(millisecondToSleep) * time.Millisecond)

		// currentTime := getCurrentTime()
		// log.Printf("Current Time: %v", currentTime)
		if nextSyncJoinTime <= getCurrentTime() {
			s.SyncJoin()
			nextSyncJoinTime = getCurrentTime() + 10
		}
		s.timestamp = getCurrentTime()
		time.Sleep(time.Duration(1000) * time.Millisecond) // always wait one second
	}
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func RunServer(configMap map[string]interface{}) {
	Health = true
	Ready = true

	services := configMap["services"].(string)
	logging.Info("Services: %v\n", services)
	port := configMap["port"].(int)
	evictable := configMap["evictable"].(bool)
	tls := configMap["tls"].(bool)
	certFile := configMap["cert"].(string)
	keyFile := configMap["key"].(string)
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		logging.Error("failed to listen: %v", err)
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
			logging.Error("Failed to generate credentials %v", err)
			return
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	grpcServer := grpc.NewServer(opts...)
	s := NewServer(services, evictable)
	pb.RegisterVeriServiceServer(grpcServer, s)
	go RestApi()
	logging.Info("Server started.")
	grpcServer.Serve(lis)
}
