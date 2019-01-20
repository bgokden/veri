package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
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

	"github.com/gaspiman/cosine_similarity"
	"github.com/gorilla/mux"
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

// This is set in compile time for optimization
const k = 1024 // 1024

const distance_mode = 1 // consine distance

type Peer struct {
	address   string
	version   string
	avg       []float64
	hist      []float64
	n         int64
	timestamp int64
}

type Client struct {
	address string
	client  *pb.VeriServiceClient
	conn    *grpc.ClientConn
}

type veriServiceServer struct {
	k                     int64
	d                     int64
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
	averageTimestamp      int64
	pointsMap             sync.Map
	services              sync.Map
	peers                 sync.Map
	knnQueryId            cache.Cache
	pointsMu              sync.RWMutex // protects points
	treeMu                sync.RWMutex // protects KDTree
	tree                  *kdtree.KDTree
	cache                 cache.Cache
	clients               sync.Map
}

type EuclideanPoint struct {
	kdtree.PointBase
	timestamp         int64
	label             string
	groupLabel        string
	sequenceLengthOne int64
	sequenceLengthTwo int64
	sequenceDimOne    int64
	sequenceDimTwo    int64
}

type EuclideanPointKey struct {
	feature           [k]float64
	groupLabel        string
	sequenceLengthOne int64
	sequenceLengthTwo int64
	sequenceDimOne    int64
	sequenceDimTwo    int64
}

type EuclideanPointValue struct {
	timestamp  int64
	label      string
	groupLabel string
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

// Return the sequenceLengthOne
func (p *EuclideanPoint) GetSequenceLengthOne() int64 {
	return p.sequenceLengthOne
}

// Return the sequenceLengthTwo
func (p *EuclideanPoint) GetSequenceLengthTwo() int64 {
	return p.sequenceLengthTwo
}

// Return the sequenceDimOne
func (p *EuclideanPoint) GetSequenceDimOne() int64 {
	return p.sequenceDimOne
}

// Return the sequenceDimTwo
func (p *EuclideanPoint) GetSequenceDimTwo() int64 {
	return p.sequenceDimTwo
}

func euclideanDistance(arr1 []float64, arr2 []float64) float64 {
	var ret float64
	for i := 0; i < len(arr1); i++ {
		tmp := arr1[i] - arr2[i]
		ret += tmp * tmp
	}
	return ret
}

func cosineDistance(arr1 []float64, arr2 []float64) float64 {
	ret, err := cosine_similarity.Cosine(arr1, arr2)
	if err != nil {
		return math.MaxFloat64
	}
	return 1 - ret
}

func vectorDistance(arr1 []float64, arr2 []float64) float64 {
	if len(arr1) != len(arr2) {
		// log.Printf("Something is very wrong")
		return math.MaxFloat64
	}
	if distance_mode == 1 {
		return cosineDistance(arr1, arr2)
	} else {
		return euclideanDistance(arr1, arr2)
	}
}

func (p *EuclideanPoint) Distance(other kdtree.Point) float64 {
	return vectorDistance(p.GetValues(), other.GetValues())
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

func NewEuclideanPointArrWithLabel(vals [k]float64,
	timestamp int64,
	label string,
	groupLabel string,
	d int64,
	sequenceLengthOne int64,
	sequenceLengthTwo int64,
	sequenceDimOne int64,
	sequenceDimTwo int64) *EuclideanPoint {
	slice := make([]float64, len(vals))
	copy(slice[:d], vals[:d])
	ret := &EuclideanPoint{
		PointBase:         kdtree.NewPointBase(slice),
		timestamp:         timestamp,
		label:             label,
		groupLabel:        groupLabel,
		sequenceLengthOne: sequenceLengthOne,
		sequenceLengthTwo: sequenceLengthTwo,
		sequenceDimOne:    sequenceDimOne,
		sequenceDimTwo:    sequenceDimTwo}
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

func (s *veriServiceServer) GetLocalData(rect *pb.GetLocalDataRequest, stream pb.VeriService_GetLocalDataServer) error {
	s.pointsMap.Range(func(key, value interface{}) bool {
		euclideanPointKey := key.(EuclideanPointKey)
		euclideanPointValue := value.(EuclideanPointValue)
		feature := &pb.Feature{
			Feature:    euclideanPointKey.feature[:s.d],
			Timestamp:  euclideanPointValue.timestamp,
			Label:      euclideanPointValue.label,
			Grouplabel: euclideanPointValue.groupLabel,
		}
		if err := stream.Send(feature); err != nil {
			// return err pass err someway
			return false
		}
		return true
	})

	return nil
}

func (s *veriServiceServer) GetKnnFromPeer(in *pb.KnnRequest, peer *Peer, featuresChannel chan<- pb.Feature) {
	log.Printf("GetKnnFromPeer %s", peer.address)
	client, err0 := s.get_client(peer.address)
	if err0 != nil {
		grpc_client := client.client
		resp, err := (*grpc_client).GetKnn(context.Background(), in)
		if err != nil {
			log.Printf("There is an error: %v", err)
			// conn.Close()
			return
		} else {
			log.Printf("Get Knn err: %v", err)
			go s.refresh_client(peer.address)
		}
		// if resp.Success {
		// log.Printf("A new Response has been received with id: %s", resp.Id)
		features := resp.GetFeatures()
		for i := 0; i < len(features); i++ {
			log.Printf("New Feature from Peer (%s) : %v", peer.address, features[i].GetLabel())
			featuresChannel <- *(features[i])
			// log.Println(features[i].GetLabel())
		}
		// conn.Close()
	}
}

func (s *veriServiceServer) GetKnnFromPeers(in *pb.KnnRequest, featuresChannel chan<- pb.Feature) {
	timeout := int64(float64(in.GetTimeout()) * 0.9)
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
				Feature:    ans[i].GetValues()[:s.d],
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
	d := int64(len(request.GetFeature()))
	var featureHash [k]float64
	copy(featureHash[:d], request.GetFeature()[:])
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
				groupLabel: feature.GetGrouplabel(),
			}
			copy(key.feature[:len(feature.Feature)], feature.Feature)
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
				euclideanPointValue.timestamp,
				euclideanPointValue.label,
				euclideanPointValue.groupLabel,
				s.d,
				euclideanPointKey.sequenceLengthOne,
				euclideanPointKey.sequenceLengthTwo,
				euclideanPointKey.sequenceDimOne,
				euclideanPointKey.sequenceDimOne)
			points = append(points, point)
		}
		tree := kdtree.NewKDTree(points)
		point := NewEuclideanPointArr(in.Feature)
		// log.Printf("len(points): %d, in.K:%d", len(points), int(in.K))
		ans := tree.KNN(point, int(in.K))
		for i := 0; i < len(ans); i++ {
			featureJson := &pb.Feature{
				Feature:           ans[i].GetValues()[:s.d],
				Timestamp:         ans[i].GetTimestamp(),
				Label:             ans[i].GetLabel(),
				Grouplabel:        ans[i].GetGroupLabel(),
				Sequencelengthone: ans[i].GetSequenceLengthOne(),
				Sequencelengthtwo: ans[i].GetSequenceLengthTwo(),
				Sequencedimone:    ans[i].GetSequenceDimOne(),
				Sequencedimtwo:    ans[i].GetSequenceDimTwo(),
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
	if s.state > 2 {
		return &pb.InsertionResponse{Code: 1}, nil
	}
	key := EuclideanPointKey{
		groupLabel:        in.GetGrouplabel(),
		sequenceLengthOne: in.GetSequencelengthone(),
		sequenceLengthTwo: in.GetSequencelengthtwo(),
		sequenceDimOne:    in.GetSequencedimone(),
		sequenceDimTwo:    in.GetSequencedimtwo(),
	}
	d := int64(len(in.GetFeature()))
	if s.d < d {
		if d > k {
			d = k // d can not be larger than maximum capacity
		}
		log.Printf("Updating current dimension to: %v", d)
		s.d = d // Maybe we can use max of
	}
	copy(key.feature[:d], in.GetFeature()[:d])
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

func (s *veriServiceServer) Join(ctx context.Context, in *pb.JoinRequest) (*pb.JoinResponse, error) {
	// log.Printf("Join request received %v", *in)
	p, _ := peer.FromContext(ctx)
	address := strings.Split(p.Addr.String(), ":")[0] + ":" + strconv.FormatInt(int64(in.GetPort()), 10)
	// log.Printf("Peer with Addr: %s called Join", address)
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
			if peerOld.timestamp > inputPeerList[i].GetTimestamp() || inputPeerList[i].GetTimestamp()+300 < getCurrentTime() {
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
		if peer.timestamp+300 > getCurrentTime() {
			peerProto := &pb.Peer{
				Address:   peer.address,
				Version:   peer.version,
				Avg:       peer.avg,
				Hist:      peer.hist,
				N:         peer.n,
				Timestamp: peer.timestamp,
			}
			outputPeerList = append(outputPeerList, peerProto)
		}
		return true
	})
	return &pb.PeerMessage{Peers: outputPeerList}, nil
}

func (s *veriServiceServer) getClient(address string) (*pb.VeriServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("fail to dial: %v", err)
		return nil, nil, err
	}
	client := pb.NewVeriServiceClient(conn)
	return &client, conn, nil
}

func (s *veriServiceServer) new_client(address string) (*Client, error) {
	client, conn, err := s.getClient(address)
	if err != nil {
		log.Printf("fail to create a client: %v", err)
		return nil, err
	}
	return &Client{
		address: address,
		client:  client,
		conn:    conn,
	}, nil
}

/*
There may be some concurrency problems where unclosed connections can occur
*/
func (s *veriServiceServer) get_client(address string) (Client, error) {
	client, ok := s.clients.Load(address)
	if ok {
		return (*(client.(*Client))), nil
	} else {
		new_client, err := s.new_client(address)
		if err != nil {
			return Client{}, err
		} else {
			s.clients.Store(address, new_client)
			return (*new_client), nil
		}
	}
	return Client{}, errors.New("Can not initilize client")
}

func (s *veriServiceServer) refresh_client(address string) {
	log.Printf("Renewing client with address %v", address)
	new_client, err := s.new_client(address)
	if err != nil {
		log.Printf("fail to get a client: %v", err) // this is probably really bad
	} else {
		s.clients.Store(address, new_client)
	}
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
		log.Printf("(callExchangeServices) There is an error %v", err)
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
	log.Printf("For peer: %v s.n: %d peer.n: %d peer timestamp: %v currentTime: %v", peer.address, s.n, peer.n, peer.timestamp, getCurrentTime())
	if peer.timestamp+360 < getCurrentTime() {
		log.Printf("Peer data is too old, maybe peer is dead: %s, peer timestamp: %d, current time: %d", peer.address, peer.timestamp, getCurrentTime())
		// Maybe remove the peer here
		s.peers.Delete(peer.address)
		s.clients.Delete(peer.address) // maybe try closing before delete
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
	distanceAvg := vectorDistance(s.avg, peer.avg)
	distanceHist := vectorDistance(s.hist, peer.hist)
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
			euclideanPointValue.timestamp,
			euclideanPointValue.label,
			euclideanPointValue.groupLabel,
			s.d,
			euclideanPointKey.sequenceLengthOne,
			euclideanPointKey.sequenceLengthTwo,
			euclideanPointKey.sequenceDimOne,
			euclideanPointKey.sequenceDimTwo)
		if rand.Float64() < 0.5 {
			if count <= limit {
				points = append(points, point)
				count++
				if count <= limit {
					return true
				} else {
					return false
				}
			}
		}
		return true
	})

	for _, point := range points {
		request := &pb.InsertionRequest{
			Timestamp:         point.GetTimestamp(),
			Label:             point.GetLabel(),
			Grouplabel:        point.GetGroupLabel(),
			Feature:           point.GetValues()[:s.d],
			Sequencelengthone: point.GetSequenceLengthOne(),
			Sequencelengthtwo: point.GetSequenceLengthTwo(),
			Sequencedimone:    point.GetSequenceDimOne(),
			Sequencedimtwo:    point.GetSequenceDimTwo(),
		}
		resp, err := (*client).Insert(context.Background(), request)
		if err != nil {
			log.Printf("There is an error: %v", err)
		} else {
			// log.Printf("A new Response has been received for %d. with code: %d", i, resp.GetCode())
			if resp.GetCode() == 0 && s.state > 0 && rand.Float64() < (0.3*float64(s.state)) {
				key := EuclideanPointKey{
					groupLabel: point.GetGroupLabel(),
				}
				copy(key.feature[:len(point.GetValues())], point.GetValues())
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
		log.Printf("(callExchangePeers) There is an error %v", err)
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
			client, err := s.get_client(serviceName)
			if err == nil {
				grpc_client := client.client
				s.callJoin(grpc_client)
			} else {
				log.Printf("SyncJoin err: %v", err)
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
			peerValue := value.(Peer)
			client, err := s.get_client(peerAddress)
			if err == nil {
				grpc_client := client.client
				s.callJoin(grpc_client)
				s.callExchangeServices(grpc_client)
				s.callExchangePeers(grpc_client)
				s.callExchangeData(grpc_client, &peerValue)
			} else {
				log.Printf("SyncJoin 2 err: %v", err)
				go s.refresh_client(peerAddress)
			}
			// conn.Close()
		}
		return true
	})
	// log.Printf("Peer loop Ended")
}

func (s *veriServiceServer) isEvictable() bool {
	if s.state >= 2 && *evictable {
		return true
	}
	return false
}

func (s *veriServiceServer) syncMapToTree() {
	// sum := make([]float64, 0)
	// count := 0
	if s.dirty || s.isEvictable() {
		s.dirty = false
		points := make([]kdtree.Point, 0)
		n := int64(0)
		distance := 0.0
		maxDistance := 0.0
		avg := make([]float64, 0)
		hist := make([]float64, 64)
		nFloat := float64(s.n)
		histUnit := 1 / nFloat
		averageTimeStamp := 0.0
		s.pointsMap.Range(func(key, value interface{}) bool {
			euclideanPointKey := key.(EuclideanPointKey)
			euclideanPointValue := value.(EuclideanPointValue)
			// In eviction mode, if a point timestamp is older than average timestamp, delete data randomly.
			if s.isEvictable() && s.averageTimestamp != 0 && euclideanPointValue.timestamp > s.averageTimestamp && rand.Float32() < 0.2 {
				s.pointsMap.Delete(key)
				return true // evict this data point
			}
			point := NewEuclideanPointArrWithLabel(
				euclideanPointKey.feature,
				euclideanPointValue.timestamp,
				euclideanPointValue.label,
				euclideanPointValue.groupLabel,
				s.d,
				euclideanPointKey.sequenceLengthOne,
				euclideanPointKey.sequenceLengthTwo,
				euclideanPointKey.sequenceDimOne,
				euclideanPointKey.sequenceDimTwo)
			points = append(points, point)
			n++
			avg = calculateAverage(avg, point, nFloat)
			averageTimeStamp = averageTimeStamp + float64(euclideanPointValue.timestamp)/nFloat
			distance = vectorDistance(s.avg, point.GetValues())
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
		log.Printf("N=%v, state=%v", n, s.state)
		s.avg = avg
		s.averageTimestamp = int64(averageTimeStamp)
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

		if nextSyncMapTime <= getCurrentTime() {
			secondsToSleep := 3 + int64((s.latestNumberOfInserts+1)%60)
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

func (s *veriServiceServer) restApi() {
	log.Println("Rest api stared")
	router := mux.NewRouter()
	router.HandleFunc("/", GetHeath).Methods("GET")
	router.HandleFunc("/health", GetHeath).Methods("GET")
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
