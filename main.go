package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/bgokden/go-kdtree"
	"github.com/istio/istio/pkg/cache"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/testdata"

	pb "github.com/bgokden/veri/veriservice"
	"github.com/segmentio/ksuid"

	"github.com/gorilla/mux"
)

var (
	tls        = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile   = flag.String("cert_file", "", "The TLS cert file")
	keyFile    = flag.String("key_file", "", "The TLS key file")
	jsonDBFile = flag.String("json_db_file", "testdata/route_guide_db.json", "A json file containing a list of features")
	port       = flag.Int("port", 10000, "The server port")
	services   = flag.String("services", "", "Comma separated list of services")
)

// This is set in compile time for optimization
const k = 300

type Peer struct {
	address   string
	version   string
	avg       []float64
	hist      []float64
	n         int64
	timestamp int64
}

type veriServiceServer struct {
	k                     int64
	avg                   []float64
	n                     int64
	maxDistance           float64
	hist                  []float64
	address               string
	version               string
	timestamp             int64
	dirty                 bool
	latestNumberOfInserts int
	state                 int
	maxMemoryMiB          uint64
	pointsMap             sync.Map
	kvMap                 sync.Map
	services              sync.Map
	peers                 sync.Map
	knnQueryId            cache.Cache
	pointsMu              sync.RWMutex // protects points
	treeMu                sync.RWMutex // protects KDTree
	tree                  *kdtree.KDTree
	cache                 cache.Cache
}

type EuclideanPoint struct {
	kdtree.PointBase
	timestamp  int64
	label      string
	groupLabel string
}

type EuclideanPointKey struct {
	feature   [k]float64
	timestamp int64
}

type EuclideanPointValue struct {
	timestamp  int64
	label      string
	groupLabel string
}

type EuclideanPointJson struct {
	Feature    []float64 `json:"feature"`
	Timestamp  int64     `json:"timestamp"`
	Label      string    `json:"label"`
	GroupLabel string    `json:"grouplabel"`
}

type ResponseJson struct {
	Result string `json:"result"`
}

type SearchJson struct {
	Feature   []float64 `json:"feature"`
	K         int64     `json:"k"`
	Timestamp int64     `json:"timestamp"`
	Timeout   int64     `json:"timeout"`
}

type SearchResultJson struct {
	Points []EuclideanPointJson `json:"points"`
}

// Return the label
func (p *EuclideanPoint) GetLabel() string {
	return p.label
}

// Return the grouplabel
func (p *EuclideanPoint) GetGroupLabel() string {
	return p.groupLabel
}

// Return the timestamp
func (p *EuclideanPoint) GetTimestamp() int64 {
	return p.timestamp
}

func euclideanDistance(arr1 []float64, arr2 []float64) float64 {
	if len(arr1) != len(arr2) {
		// log.Printf("Something is very wrong")
		return 0
	}
	var ret float64
	for i := 0; i < len(arr1); i++ {
		tmp := arr1[i] - arr2[i]
		ret += tmp * tmp
	}
	return ret
}

func (p *EuclideanPoint) Distance(other kdtree.Point) float64 {
	var ret float64
	for i := 0; i < p.Dim(); i++ {
		tmp := p.GetValue(i) - other.GetValue(i)
		ret += tmp * tmp
	}
	return ret
}

func (p *EuclideanPoint) PlaneDistance(val float64, dim int) float64 {
	tmp := p.GetValue(dim) - val
	return tmp * tmp
}

func NewEuclideanPoint(vals ...float64) *EuclideanPoint {
	ret := &EuclideanPoint{
		PointBase: kdtree.NewPointBase(vals),
	}
	return ret
}

func NewEuclideanPointWithLabel(timestamp int64, label string, vals ...float64) *EuclideanPoint {
	ret := &EuclideanPoint{
		PointBase: kdtree.NewPointBase(vals),
		timestamp: timestamp,
		label:     label,
	}
	return ret
}

func NewEuclideanPointArr(vals []float64) *EuclideanPoint {
	ret := &EuclideanPoint{
		PointBase: kdtree.NewPointBase(vals),
	}
	return ret
}

func NewEuclideanPointArrWithLabel(vals [k]float64, timestamp int64, label string, groupLabel string) *EuclideanPoint {
	slice := make([]float64, k)
	copy(slice, vals[:])
	ret := &EuclideanPoint{
		PointBase:  kdtree.NewPointBase(slice),
		timestamp:  timestamp,
		label:      label,
		groupLabel: groupLabel,
	}
	return ret
}

func equal(p1 kdtree.Point, p2 kdtree.Point) bool {
	for i := 0; i < p1.Dim(); i++ {
		if p1.GetValue(i) != p2.GetValue(i) {
			return false
		}
	}
	return true
}

func getCurrentTime() int64 {
	return time.Now().Unix()
}

func calculateAverage(avg []float64, p kdtree.Point, n float64) []float64 {
	if n == 0 {
		return p.GetValues()
	}
	if len(avg) < p.Dim() {
		avg = make([]float64, p.Dim())
	}
	for i := 0; i < p.Dim(); i++ {
		avg[i] += p.GetValue(i) / n
	}
	return avg
}

func (s *veriServiceServer) GetKnnFromPeer(in *pb.KnnRequest, peer *Peer, featuresChannel chan<- pb.Feature) {
	log.Printf("GetKnnFromPeer %s", peer.address)
	client, conn := s.getClient(peer.address)
	resp, err := (*client).GetKnn(context.Background(), in)
	if err != nil {
		log.Printf("There is an error: %v", err)
		conn.Close()
		return
	}
	// if resp.Success {
	// log.Printf("A new Response has been received with id: %s", resp.Id)
	features := resp.GetFeatures()
	for i := 0; i < len(features); i++ {
		log.Printf("New Feature from Peer (%s) : %v", peer.address, features[i].GetLabel())
		featuresChannel <- *(features[i])
		// log.Println(features[i].GetLabel())
	}
	conn.Close()
}

func (s *veriServiceServer) GetKnnFromPeers(in *pb.KnnRequest, featuresChannel chan<- pb.Feature) {
	timeout := int64(float64(in.GetTimeout()) / 2.0)
	request := &pb.KnnRequest{
		Feature:   in.GetFeature(),
		Id:        in.GetId(),
		K:         in.GetK(),
		Timestamp: in.GetTimestamp(),
		Timeout:   timeout,
	}
	log.Printf("GetKnnFromPeers")
	s.peers.Range(func(key, value interface{}) bool {
		peerAddress := key.(string)
		log.Printf("Peer %s", peerAddress)
		if len(peerAddress) > 0 && peerAddress != s.address {
			peerValue := value.(Peer)
			s.GetKnnFromPeer(request, &peerValue, featuresChannel)
		}
		return true
	})
}

func (s *veriServiceServer) GetKnnFromLocal(in *pb.KnnRequest, featuresChannel chan<- pb.Feature) {
	log.Printf("GetKnnFromLocal")
	point := NewEuclideanPointArr(in.GetFeature())
	if s.tree != nil {
		s.treeMu.RLock()
		ans := s.tree.KNN(point, int(in.GetK()))
		s.treeMu.RUnlock()
		for i := 0; i < len(ans); i++ {
			feature := &pb.Feature{
				Feature:    ans[i].GetValues(),
				Timestamp:  ans[i].GetTimestamp(),
				Label:      ans[i].GetLabel(),
				Grouplabel: ans[i].GetGroupLabel(),
			}
			log.Printf("New Feature from Local: %v", feature.GetLabel())
			featuresChannel <- *feature
		}
	}
}

// Do a distributed Knn search
func (s *veriServiceServer) GetKnn(ctx context.Context, in *pb.KnnRequest) (*pb.KnnResponse, error) {
	request := *in
	var featureHash [k]float64
	copy(featureHash[:], request.GetFeature()[:])
	if len(in.GetId()) == 0 {
		request.Id = ksuid.New().String()
		s.knnQueryId.Set(request.Id, true)
	} else {
		_, loaded := s.knnQueryId.Get(request.GetId())
		if loaded {
			cachedResult, isCached := s.cache.Get(featureHash)
			if isCached {
				return cachedResult.(*pb.KnnResponse), nil
			} else {
				return &pb.KnnResponse{Id: in.Id, Features: nil}, nil
			}
		} else {
			s.knnQueryId.Set(request.GetId(), getCurrentTime())
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
	reduceMap := make(map[EuclideanPointKey]EuclideanPointValue)
	for dataAvailable {
		select {
		case feature := <-featuresChannel:
			key := EuclideanPointKey{
				timestamp: feature.Timestamp,
			}
			copy(key.feature[:], feature.Feature)
			value := EuclideanPointValue{
				timestamp:  feature.Timestamp,
				label:      feature.Label,
				groupLabel: feature.Grouplabel,
			}
			reduceMap[key] = value
		case <-timeLimit:
			log.Printf("timeout")
			dataAvailable = false
			break
		}
	}
	if len(reduceMap) > 0 {
		points := make([]kdtree.Point, 0)
		for euclideanPointKey, euclideanPointValue := range reduceMap {
			log.Printf("Received Feature (Get KNN): %v", euclideanPointValue.label)
			point := NewEuclideanPointArrWithLabel(
				euclideanPointKey.feature,
				euclideanPointKey.timestamp,
				euclideanPointValue.label,
				euclideanPointValue.groupLabel)
			points = append(points, point)
		}
		tree := kdtree.NewKDTree(points)
		point := NewEuclideanPointArr(in.Feature)
		// log.Printf("len(points): %d, in.K:%d", len(points), int(in.K))
		ans := tree.KNN(point, int(in.K))
		for i := 0; i < len(ans); i++ {
			featureJson := &pb.Feature{
				Feature:    ans[i].GetValues(),
				Timestamp:  ans[i].GetTimestamp(),
				Label:      ans[i].GetLabel(),
				Grouplabel: ans[i].GetGroupLabel(),
			}
			log.Printf("New Feature (Get Knn): %v", ans[i].GetLabel())
			responseFeatures = append(responseFeatures, featureJson)
		}
	}
	s.knnQueryId.Set(request.GetId(), true)
	s.cache.Set(featureHash, &pb.KnnResponse{Id: request.GetId(), Features: responseFeatures})
	return &pb.KnnResponse{Id: request.GetId(), Features: responseFeatures}, nil
}

func (s *veriServiceServer) Insert(ctx context.Context, in *pb.InsertionRequest) (*pb.InsertionResponse, error) {
	if s.state > 1 {
		return &pb.InsertionResponse{Code: 1}, nil
	}
	key := EuclideanPointKey{
		timestamp: in.GetTimestamp(),
	}
	copy(key.feature[:], in.GetFeature())
	value := EuclideanPointValue{
		timestamp:  in.GetTimestamp(),
		label:      in.GetLabel(),
		groupLabel: in.GetGrouplabel(),
	}
	s.pointsMap.Store(key, value)
	s.dirty = true
	s.latestNumberOfInserts++
	return &pb.InsertionResponse{Code: 0}, nil
}

func (s *veriServiceServer) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	feature, ok := s.kvMap.Load(in.GetLabel())
	if ok {
		protoFeature := feature.([k]float64)
		return &pb.GetResponse{Code: 0, Feature: protoFeature[:]}, nil
	} else {
		return s.GetFromPeers(ctx, in)
	}
	return &pb.GetResponse{Code: 1}, nil
}

func (s *veriServiceServer) GetFromPeers(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	log.Printf("GetFromPeers")
	response := &pb.GetResponse{Code: 2}
	s.peers.Range(func(key, value interface{}) bool {
		peerAddress := key.(string)
		log.Printf("Peer %s", peerAddress)
		if len(peerAddress) > 0 && peerAddress != s.address {
			peerValue := value.(Peer)
			client, conn := s.getClient(peerValue.address)
			resp, err := (*client).Get(context.Background(), in)
			if err != nil {
				log.Printf("There is an error: %v", err)
				conn.Close()
			}
			if resp.GetCode() == 0 {
				response = resp
			}
			conn.Close()
		}
		return true
	})
	return response, nil
}

func (s *veriServiceServer) Join(ctx context.Context, in *pb.JoinRequest) (*pb.JoinResponse, error) {
	// log.Printf("Join request received %v", *in)
	p, _ := peer.FromContext(ctx)
	address := strings.Split(p.Addr.String(), ":")[0] + ":" + strconv.FormatInt(int64(in.GetPort()), 10)
	// log.Printf("Peer Addr: %s", address)
	peer := Peer{
		address:   address,
		avg:       in.GetAvg(),
		version:   in.GetVersion(),
		hist:      in.GetHist(),
		n:         in.GetN(),
		timestamp: in.GetTimestamp(),
	}
	s.peers.Store(address, peer)
	return &pb.JoinResponse{Address: address}, nil
}

func (s *veriServiceServer) ExchangeServices(ctx context.Context, in *pb.ServiceMessage) (*pb.ServiceMessage, error) {
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

func (s *veriServiceServer) ExchangePeers(ctx context.Context, in *pb.PeerMessage) (*pb.PeerMessage, error) {
	inputPeerList := in.GetPeers()
	for i := 0; i < len(inputPeerList); i++ {
		insert := true
		temp, ok := s.peers.Load(inputPeerList[i].GetAddress())
		if ok {
			peerOld := temp.(Peer)
			if peerOld.timestamp > inputPeerList[i].GetTimestamp() {
				insert = false
			}
		}
		if insert {
			peer := Peer{
				address:   inputPeerList[i].GetAddress(),
				version:   inputPeerList[i].GetVersion(),
				avg:       inputPeerList[i].GetAvg(),
				hist:      inputPeerList[i].GetHist(),
				n:         inputPeerList[i].GetN(),
				timestamp: inputPeerList[i].GetTimestamp(),
			}
			s.peers.Store(inputPeerList[i].GetAddress(), peer)
		}
	}
	outputPeerList := make([]*pb.Peer, 0)
	s.peers.Range(func(key, value interface{}) bool {
		// address := key.(string)
		peer := value.(Peer)
		peerProto := &pb.Peer{
			Address:   peer.address,
			Version:   peer.version,
			Avg:       peer.avg,
			Hist:      peer.hist,
			N:         peer.n,
			Timestamp: peer.timestamp,
		}
		outputPeerList = append(outputPeerList, peerProto)
		return true
	})
	return &pb.PeerMessage{Peers: outputPeerList}, nil
}

func (s *veriServiceServer) getClient(address string) (*pb.VeriServiceClient, *grpc.ClientConn) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	client := pb.NewVeriServiceClient(conn)
	return &client, conn
}

func (s *veriServiceServer) callJoin(client *pb.VeriServiceClient) {
	request := &pb.JoinRequest{
		Address:   s.address,
		Avg:       s.avg,
		Port:      int32(*port),
		Version:   s.version,
		Hist:      s.hist,
		N:         s.n,
		Timestamp: s.timestamp,
	}
	// log.Printf("Call Join Request %v", *request)
	resp, err := (*client).Join(context.Background(), request)
	if err != nil {
		log.Printf("(Call Join) There is an error %v", err)
		return
	}
	if s.address != resp.GetAddress() {
		s.address = resp.GetAddress()
	}
}

func (s *veriServiceServer) callExchangeServices(client *pb.VeriServiceClient) {
	outputServiceList := make([]string, 0)
	s.services.Range(func(key, value interface{}) bool {
		serviceName := key.(string)
		outputServiceList = append(outputServiceList, serviceName)
		return true
	})
	request := &pb.ServiceMessage{
		Services: outputServiceList,
	}
	resp, err := (*client).ExchangeServices(context.Background(), request)
	if err != nil {
		log.Printf("There is an error %v", err)
		return
	}
	inputServiceList := resp.GetServices()
	for i := 0; i < len(inputServiceList); i++ {
		s.services.Store(inputServiceList[i], true)
	}
	// log.Printf("Services exhanged")
}

func (s *veriServiceServer) callExchangeData(client *pb.VeriServiceClient, peer *Peer) {
	// need to check avg and hist differences ...
	// chose datum
	// log.Printf("s.n: %d   peer.n: %d", s.n, peer.n)
	if peer.timestamp+360 < getCurrentTime() {
		log.Printf("Peer data is too old, maybe peer is dead: %s, peer timestamp: %d, current time: %d", peer.address, peer.timestamp, getCurrentTime())
		// Maybe remove the peer here
		s.peers.Delete(peer.address)
		return
	}
	if peer.timestamp+30 < getCurrentTime() && s.state == 0 {
		// log.Printf("Peer data is too old: %s", peer.address)
		// limit = 1 // no change can be risky
		return
	}
	if s.n < peer.n {
		// log.Printf("Other peer should initiate exchange data %s", peer.address)
		return
	}
	distanceAvg := euclideanDistance(s.avg, peer.avg)
	distanceHist := euclideanDistance(s.hist, peer.hist)
	// log.Printf("%s => distanceAvg %f, distanceHist: %f", peer.address, distanceAvg, distanceHist)
	limit := int(((s.n - peer.n) / 10) % 1000)
	nRatio := 0.0
	if peer.n != 0 {
		nRatio = float64(s.n) / float64(peer.n)
	}
	if 0.99 < nRatio && nRatio < 1.01 && distanceAvg < 0.0005 && distanceHist < 0.0005 && s.state == 0 {
		// log.Printf("Decrease number of changes to 1 since stats are close enough %s", peer.address)
		limit = 1 // no change can be risky
	}
	count := 0
	points := make([]kdtree.Point, 0)
	s.pointsMap.Range(func(key, value interface{}) bool {
		euclideanPointKey := key.(EuclideanPointKey)
		euclideanPointValue := value.(EuclideanPointValue)
		point := NewEuclideanPointArrWithLabel(
			euclideanPointKey.feature,
			euclideanPointKey.timestamp,
			euclideanPointValue.label,
			euclideanPointValue.groupLabel)
		if count <= limit {
			points = append(points, point)
		}
		count++
		return true
	})

	for _, point := range points {
		request := &pb.InsertionRequest{
			Timestamp:  point.GetTimestamp(),
			Label:      point.GetLabel(),
			Grouplabel: point.GetGroupLabel(),
			Feature:    point.GetValues(),
		}
		resp, err := (*client).Insert(context.Background(), request)
		if err != nil {
			log.Printf("There is an error: %v", err)
		} else {
			// log.Printf("A new Response has been received for %d. with code: %d", i, resp.GetCode())
			if resp.GetCode() == 0 && s.state > 0 {
				key := EuclideanPointKey{
					timestamp: point.GetTimestamp(),
				}
				copy(key.feature[:], point.GetValues())
				s.pointsMap.Delete(key)
				s.dirty = true
			}
		}
	}
}

func (s *veriServiceServer) callExchangePeers(client *pb.VeriServiceClient) {
	// log.Printf("callExchangePeers")
	outputPeerList := make([]*pb.Peer, 0)
	s.peers.Range(func(key, value interface{}) bool {
		// address := key.(string)
		peer := value.(Peer)
		peerProto := &pb.Peer{
			Address:   peer.address,
			Version:   peer.version,
			Avg:       peer.avg,
			Hist:      peer.hist,
			N:         peer.n,
			Timestamp: peer.timestamp,
		}
		outputPeerList = append(outputPeerList, peerProto)
		return true
	})
	request := &pb.PeerMessage{
		Peers: outputPeerList,
	}
	resp, err := (*client).ExchangePeers(context.Background(), request)
	if err != nil {
		log.Printf("There is an error %v", err)
		return
	}
	inputPeerList := resp.GetPeers()
	for i := 0; i < len(inputPeerList); i++ {
		insert := true
		temp, ok := s.peers.Load(inputPeerList[i].GetAddress())
		if ok {
			peerOld := temp.(Peer)
			if peerOld.timestamp > inputPeerList[i].GetTimestamp() {
				insert = false
			}
		}
		if insert && s.address != inputPeerList[i].GetAddress() {
			peer := Peer{
				address:   inputPeerList[i].GetAddress(),
				version:   inputPeerList[i].GetVersion(),
				avg:       inputPeerList[i].GetAvg(),
				hist:      inputPeerList[i].GetHist(),
				n:         inputPeerList[i].GetN(),
				timestamp: inputPeerList[i].GetTimestamp(),
			}
			s.peers.Store(inputPeerList[i].GetAddress(), peer)
		}
	}
	// log.Printf("Peers exhanged")
}

func (s *veriServiceServer) SyncJoin() {
	// log.Printf("Sync Join")
	s.services.Range(func(key, value interface{}) bool {
		serviceName := key.(string)
		// log.Printf("Service %s", serviceName)
		if len(serviceName) > 0 {
			client, conn := s.getClient(serviceName)
			s.callJoin(client)
			conn.Close()
		}
		return true
	})
	// log.Printf("Service loop Ended")
	s.peers.Range(func(key, value interface{}) bool {
		peerAddress := key.(string)
		// log.Printf("Peer %s", peerAddress)
		if len(peerAddress) > 0 && peerAddress != s.address {
			peerValue := value.(Peer)
			client, conn := s.getClient(peerAddress)
			s.callJoin(client)
			s.callExchangeServices(client)
			s.callExchangePeers(client)
			s.callExchangeData(client, &peerValue)
			conn.Close()
		}
		return true
	})
	// log.Printf("Peer loop Ended")
}

func (s *veriServiceServer) syncMapToTree() {
	// sum := make([]float64, 0)
	// count := 0
	if s.dirty {
		s.dirty = false
		points := make([]kdtree.Point, 0)
		n := int64(0)
		distance := 0.0
		maxDistance := 0.0
		avg := make([]float64, 0)
		hist := make([]float64, 64)
		nFloat := float64(s.n)
		histUnit := 1 / nFloat
		var tempkvMap sync.Map
		s.pointsMap.Range(func(key, value interface{}) bool {
			euclideanPointKey := key.(EuclideanPointKey)
			euclideanPointValue := value.(EuclideanPointValue)
			point := NewEuclideanPointArrWithLabel(
				euclideanPointKey.feature,
				euclideanPointKey.timestamp,
				euclideanPointValue.label,
				euclideanPointValue.groupLabel)
			points = append(points, point)
			tempkvMap.Store(euclideanPointValue.label, euclideanPointKey.feature)
			n++
			avg = calculateAverage(avg, point, nFloat)
			distance = euclideanDistance(s.avg, point.GetValues())
			if distance > maxDistance {
				maxDistance = distance
			}
			if s.maxDistance != 0 {
				index := int((distance / s.maxDistance) * 64)
				if index >= 64 {
					index = 63
				}
				hist[index] += histUnit
			}
			return true
		})
		// log.Printf("Max Distance %f", maxDistance)
		// log.Printf("hist")
		// log.Print(hist)
		// log.Printf("avg")
		// log.Print(avg)
		s.kvMap = tempkvMap
		s.avg = avg
		s.hist = hist
		s.maxDistance = maxDistance
		s.n = n
		s.timestamp = getCurrentTime()
		s.latestNumberOfInserts = 0
		if len(points) > 0 {
			tree := kdtree.NewKDTree(points)
			s.treeMu.Lock()
			s.tree = tree
			s.treeMu.Unlock()
		}
	}
	s.timestamp = getCurrentTime() // update always
}

func newServer() *veriServiceServer {
	s := &veriServiceServer{}
	log.Printf("services %s", *services)
	serviceList := strings.Split(*services, ",")
	for _, service := range serviceList {
		if len(service) > 0 {
			s.services.Store(service, true)
		}
	}
	s.maxMemoryMiB = 1024
	s.timestamp = getCurrentTime()
	s.cache = cache.NewLRU(5*time.Minute, 5*time.Minute, 1000)
	s.knnQueryId = cache.NewLRU(5*time.Minute, 5*time.Minute, 1000)
	return s
}

func (s *veriServiceServer) check() {
	nextSyncJoinTime := getCurrentTime()
	nextSyncMapTime := getCurrentTime()
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
		} else if currentMemory < maxMemory*0.85 {
			s.state = 1 // Accept insert, delete while sending data
		} else {
			s.state = 2 // Don't accept insert, delete while sending data
		}
		// log.Printf("Current Memory = %f MiB => current State %d", currentMemory, s.state)
		// millisecondToSleep := int64(((s.latestNumberOfInserts + 100) % 1000) * 10)
		// log.Printf("millisecondToSleep: %d, len %d", millisecondToSleep, s.n)
		// time.Sleep(time.Duration(millisecondToSleep) * time.Millisecond)

		// currentTime := getCurrentTime()
		// log.Printf("Current Time: %v", currentTime)
		if nextSyncJoinTime <= getCurrentTime() {
			s.SyncJoin()
			nextSyncJoinTime = getCurrentTime() + 1
		}

		if nextSyncMapTime <= getCurrentTime() {
			secondsToSleep := int64((s.latestNumberOfInserts + 1) % 60)
			s.syncMapToTree()
			nextSyncMapTime = getCurrentTime() + secondsToSleep
		}
		time.Sleep(time.Duration(1000) * time.Millisecond) // always wait one second
	}
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func GetHeath(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, `{"alive": true}`)
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

func (s *veriServiceServer) PostInsert(w http.ResponseWriter, r *http.Request) {
	if s.state > 1 {
		respondWithError(w, http.StatusTooManyRequests, "Too many requests")
		return
	}
	var in EuclideanPointJson
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&in); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}
	defer r.Body.Close()
	key := EuclideanPointKey{
		timestamp: in.Timestamp,
	}
	copy(key.feature[:], in.Feature)
	value := EuclideanPointValue{
		timestamp:  in.Timestamp,
		label:      in.Label,
		groupLabel: in.GroupLabel,
	}
	s.pointsMap.Store(key, value)
	s.dirty = true
	s.latestNumberOfInserts++
	respondWithJSON(w, http.StatusOK, ResponseJson{Result: "Ok"})
}

func (s *veriServiceServer) PostSearch(w http.ResponseWriter, r *http.Request) {
	var in SearchJson
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&in); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}
	defer r.Body.Close()
	id := ksuid.New().String()
	s.knnQueryId.Set(id, getCurrentTime())
	request := &pb.KnnRequest{
		Id:        id,
		Timestamp: in.Timestamp,
		Timeout:   in.Timeout,
		K:         int32(in.K),
		Feature:   in.Feature,
	}
	featuresChannel := make(chan pb.Feature, 100) // in.K
	go s.GetKnnFromPeers(request, featuresChannel)
	go s.GetKnnFromLocal(request, featuresChannel)
	// time.Sleep(1 * time.Second)
	// close(featuresChannel)
	responseFeatures := make([]EuclideanPointJson, 0)
	dataAvailable := true
	timeLimit := time.After(time.Duration(in.Timeout) * time.Millisecond)
	reduceMap := make(map[EuclideanPointKey]EuclideanPointValue)
	for dataAvailable {
		select {
		case feature := <-featuresChannel:
			key := EuclideanPointKey{
				timestamp: feature.Timestamp,
			}
			copy(key.feature[:], feature.Feature)
			value := EuclideanPointValue{
				timestamp:  feature.Timestamp,
				label:      feature.Label,
				groupLabel: feature.Grouplabel,
			}
			reduceMap[key] = value
		case <-timeLimit:
			log.Printf("timeout PostSearch")
			dataAvailable = false
			break
		}
	}
	if len(reduceMap) > 0 {
		points := make([]kdtree.Point, 0)
		for euclideanPointKey, euclideanPointValue := range reduceMap {
			log.Printf("Received Feature (PostSearch): %v", euclideanPointValue.label)
			point := NewEuclideanPointArrWithLabel(
				euclideanPointKey.feature,
				euclideanPointKey.timestamp,
				euclideanPointValue.label,
				euclideanPointValue.groupLabel)
			points = append(points, point)
		}
		tree := kdtree.NewKDTree(points)
		point := NewEuclideanPointArr(in.Feature)
		ans := tree.KNN(point, int(in.K))
		for i := 0; i < len(ans); i++ {
			featureJson := EuclideanPointJson{
				Feature:    ans[i].GetValues(),
				Timestamp:  ans[i].GetTimestamp(),
				Label:      ans[i].GetLabel(),
				GroupLabel: ans[i].GetGroupLabel(),
			}
			log.Printf("New Feature (PostSearch): %v", ans[i].GetLabel())
			responseFeatures = append(responseFeatures, featureJson)
		}
	}
	respondWithJSON(w, http.StatusOK, SearchResultJson{Points: responseFeatures})
}

func (s *veriServiceServer) restApi() {
	log.Println("Rest api stared")
	router := mux.NewRouter()
	router.HandleFunc("/", GetHeath).Methods("GET")
	router.HandleFunc("/health", GetHeath).Methods("GET")
	router.HandleFunc("/v1/insert", s.PostInsert).Methods("POST")
	router.HandleFunc("/v1/search", s.PostSearch).Methods("POST")
	log.Fatal(http.ListenAndServe(":8000", router))
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
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
			log.Fatalf("Failed to generate credentials %v", err)
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	grpcServer := grpc.NewServer(opts...)
	s := newServer()
	pb.RegisterVeriServiceServer(grpcServer, s)
	go s.check()
	go s.restApi()
	log.Println("Server started .")
	grpcServer.Serve(lis)
}
