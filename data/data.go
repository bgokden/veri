package data

import (
	bytes "bytes"
	gob "encoding/gob"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	kdtree "github.com/bgokden/go-kdtree"
	pb "github.com/bgokden/veri/veriservice"
	badger "github.com/dgraph-io/badger"
	"github.com/gaspiman/cosine_similarity"
)

type Data struct {
	K                     int64
	D                     int64
	Avg                   []float64
	N                     int64
	MaxDistance           float64
	Hist                  []float64
	timestamp             int64
	averageTimestamp      int64
	dirty                 bool
	latestNumberOfChanges int
	treeMu                sync.RWMutex // protects KDTree
	tree                  *kdtree.KDTree
	IsEvictable           bool
	DB                    *badger.DB
	DBPath                string
}

// NewData
func NewData(path string) *Data {
	dt := Data{}
	log.Printf("Create Data\n")
	dt.DBPath = path // "/tmp/veri"
	db, err := badger.Open(badger.DefaultOptions(dt.DBPath))
	if err != nil {
		log.Fatal(err)
	}
	dt.DB = db
	go dt.Run()
	go func() {
		sigint := make(chan os.Signal, 1)

		// interrupt signal sent from terminal
		signal.Notify(sigint, os.Interrupt)
		// sigterm signal sent from kubernetes
		signal.Notify(sigint, syscall.SIGTERM)

		<-sigint

		dt.DB.Close()
	}()
	return &dt
}

// Close
func (dt *Data) Close() error {
	return dt.DB.Close()
}

type Stats struct {
	K                int64
	D                int64
	Avg              []float64
	N                int64
	MaxDistance      float64
	Hist             []float64
	Timestamp        int64
	AverageTimestamp int64
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
	Feature           []float64
	SequenceLengthOne int64
	SequenceLengthTwo int64
	SequenceDimOne    int64
	SequenceDimTwo    int64
	GroupLabel        string
}

type EuclideanPointValue struct {
	Timestamp  int64
	Label      string
	GroupLabel string
}

// Return the label
func (p *EuclideanPoint) GetValue(dim int) float64 {
	if dim < p.Dim() {
		return p.Vec[dim]
	}
	return 0
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

// EncodeEuclideanPointKey serializes EuclideanPointKey
func EncodeEuclideanPointKey(p *EuclideanPointKey) []byte {
	var byteBuffer bytes.Buffer
	encoder := gob.NewEncoder(&byteBuffer)
	if err := encoder.Encode(*p); err != nil {
		log.Printf("Encoding error %v\n", err)
	}
	return byteBuffer.Bytes()
}

// DecodeEuclideanPointKey de-serializes EuclideanPointKey
func DecodeEuclideanPointKey(byteArray []byte) *EuclideanPointKey {
	var element EuclideanPointKey
	r := bytes.NewReader(byteArray)
	decoder := gob.NewDecoder(r)
	if err := decoder.Decode(&element); err != nil {
		log.Printf("Encoding error %v\n", err)
	}
	return &element
}

// EncodeEuclideanPointValue serializes EuclideanPointValue
func EncodeEuclideanPointValue(p *EuclideanPointValue) []byte {
	var byteBuffer bytes.Buffer
	encoder := gob.NewEncoder(&byteBuffer)
	if err := encoder.Encode(*p); err != nil {
		log.Printf("Encoding error %v\n", err)
	}
	return byteBuffer.Bytes()
}

// DecodeEuclideanPointValue de-serializes EuclideanPointValue
func DecodeEuclideanPointValue(byteArray []byte) *EuclideanPointValue {
	var element EuclideanPointValue
	r := bytes.NewReader(byteArray)
	decoder := gob.NewDecoder(r)
	if err := decoder.Decode(&element); err != nil {
		log.Printf("Encoding error %v\n", err)
	}
	return &element
}

func euclideanDistance(arr1 []float64, arr2 []float64) float64 {
	var ret float64
	for i := 0; i < len(arr1); i++ {
		tmp := arr1[i] - arr2[i]
		ret += tmp * tmp
	}
	return ret
}

func euclideanDistanceWithBasicExtremeDetection(arr1 []float64, arr2 []float64) float64 {
	diffArr := make([]float64, len(arr1), len(arr1))
	length := float64(len(arr1))
	avg := 0.0
	for i := 0; i < len(arr1); i++ {
		tmp := arr1[i] - arr2[i]
		sq := tmp * tmp
		avg += sq / length
		diffArr[i] = sq
	}
	std := 0.0
	for i := 0; i < len(diffArr); i++ {
		std += ((diffArr[i] - avg) * (diffArr[i] - avg)) / length
	}
	for i := 0; i < len(diffArr); i++ {
		if diffArr[i] >= avg+2*std {
			diffArr[i] = 2 * diffArr[i]
		} else if diffArr[i] <= avg-2*std {
			diffArr[i] = 2 * diffArr[i]
		}
	}
	return sum(diffArr)
}

func cosineDistance(arr1 []float64, arr2 []float64) float64 {
	ret, err := cosine_similarity.Cosine(arr1, arr2)
	if err != nil {
		return math.MaxFloat64
	}
	return 1 - ret
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func sum(arr []float64) float64 {
	sum := 0.0
	for _, e := range arr {
		sum += e
	}
	return sum
}

// VectorDistance
func VectorDistance(arr1 []float64, arr2 []float64) float64 {
	minLen := min(len(arr1), len(arr2))
	d := euclideanDistance(arr1[:minLen], arr2[:minLen])
	return d
}

func (p *EuclideanPoint) Distance(other kdtree.Point) float64 {
	return VectorDistance(p.GetValues(), other.GetValues())
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

func NewEuclideanPointArrWithLabel(vals []float64,
	timestamp int64,
	label string,
	groupLabel string,
	sequenceLengthOne int64,
	sequenceLengthTwo int64,
	sequenceDimOne int64,
	sequenceDimTwo int64) *EuclideanPoint {
	ret := &EuclideanPoint{
		PointBase:         kdtree.NewPointBase(vals),
		timestamp:         timestamp,
		label:             label,
		groupLabel:        groupLabel,
		sequenceLengthOne: sequenceLengthOne,
		sequenceLengthTwo: sequenceLengthTwo,
		sequenceDimOne:    sequenceDimOne,
		sequenceDimTwo:    sequenceDimTwo}
	return ret
}

func NewEuclideanPointFromFeature(feature *pb.Feature) *EuclideanPoint {
	ret := &EuclideanPoint{
		PointBase:         kdtree.NewPointBase(feature.Feature),
		timestamp:         feature.GetTimestamp(),
		label:             feature.GetLabel(),
		groupLabel:        feature.GetGrouplabel(),
		sequenceLengthOne: feature.GetSequencelengthone(),
		sequenceLengthTwo: feature.GetSequencelengthtwo(),
		sequenceDimOne:    feature.GetSequencedimone(),
		sequenceDimTwo:    feature.GetSequencedimtwo(),
	}
	return ret
}

func NewFeatureFromEuclideanPoint(point *EuclideanPoint) *pb.Feature {
	ret := &pb.Feature{
		Feature:           point.GetValues(),
		Timestamp:         point.GetTimestamp(),
		Label:             point.GetLabel(),
		Grouplabel:        point.GetGroupLabel(),
		Sequencelengthone: point.GetSequenceLengthOne(),
		Sequencelengthtwo: point.GetSequenceLengthTwo(),
		Sequencedimone:    point.GetSequenceDimOne(),
		Sequencedimtwo:    point.GetSequenceDimTwo(),
	}
	return ret
}

func NewFeatureFromPoint(point kdtree.Point) *pb.Feature {
	ret := &pb.Feature{
		Feature:           point.GetValues(),
		Timestamp:         point.GetTimestamp(),
		Label:             point.GetLabel(),
		Grouplabel:        point.GetGroupLabel(),
		Sequencelengthone: point.GetSequenceLengthOne(),
		Sequencelengthtwo: point.GetSequenceLengthTwo(),
		Sequencedimone:    point.GetSequenceDimOne(),
		Sequencedimtwo:    point.GetSequenceDimTwo(),
	}
	return ret
}

func NewInsertionRequestFromPoint(point kdtree.Point) *pb.InsertionRequest {
	ret := &pb.InsertionRequest{
		Timestamp:         point.GetTimestamp(),
		Label:             point.GetLabel(),
		Grouplabel:        point.GetGroupLabel(),
		Feature:           point.GetValues(),
		Sequencelengthone: point.GetSequenceLengthOne(),
		Sequencelengthtwo: point.GetSequenceLengthTwo(),
		Sequencedimone:    point.GetSequenceDimOne(),
		Sequencedimtwo:    point.GetSequenceDimTwo(),
	}
	return ret
}

func FeatureToEuclideanPointKeyValue(feature *pb.Feature) (*EuclideanPointKey, *EuclideanPointValue) {
	key := &EuclideanPointKey{
		Feature:           feature.GetFeature(),
		GroupLabel:        feature.GetGrouplabel(),
		SequenceLengthOne: feature.GetSequencelengthone(),
		SequenceLengthTwo: feature.GetSequencelengthtwo(),
		SequenceDimOne:    feature.GetSequencedimone(),
		SequenceDimTwo:    feature.GetSequencedimtwo(),
	}
	value := &EuclideanPointValue{
		Timestamp:  feature.GetTimestamp(),
		Label:      feature.GetLabel(),
		GroupLabel: feature.GetGrouplabel(),
	}
	return key, value
}

func InsertionRequestToEuclideanPointKeyValue(in *pb.InsertionRequest) (*EuclideanPointKey, *EuclideanPointValue) {
	key := &EuclideanPointKey{
		Feature:           in.GetFeature(),
		GroupLabel:        in.GetGrouplabel(),
		SequenceLengthOne: in.GetSequencelengthone(),
		SequenceLengthTwo: in.GetSequencelengthtwo(),
		SequenceDimOne:    in.GetSequencedimone(),
		SequenceDimTwo:    in.GetSequencedimtwo(),
	}
	value := &EuclideanPointValue{
		Timestamp:  in.GetTimestamp(),
		Label:      in.GetLabel(),
		GroupLabel: in.GetGrouplabel(),
	}
	return key, value
}

func NewEuclideanPointFromKeyValue(key *EuclideanPointKey, value *EuclideanPointValue) *EuclideanPoint {
	ret := NewEuclideanPointArrWithLabel(
		key.Feature,
		value.Timestamp,
		value.Label,
		value.GroupLabel,
		key.SequenceLengthOne,
		key.SequenceLengthTwo,
		key.SequenceDimOne,
		key.SequenceDimTwo)
	return ret
}

func FeatureFromEuclideanKeyValue(key *EuclideanPointKey, value *EuclideanPointValue) *pb.Feature {
	d := (key.SequenceLengthOne * key.SequenceDimOne) + (key.SequenceLengthTwo * key.SequenceDimTwo)
	if d == 0 {
		fmt.Printf("FeatureFromEuclideanKeyValue: D is 0 !!!!!!!!!!\n")
	}
	ret := &pb.Feature{
		Feature:           key.Feature[:d],
		Timestamp:         value.Timestamp,
		Label:             value.Label,
		Grouplabel:        value.GroupLabel,
		Sequencelengthone: key.SequenceLengthOne,
		Sequencelengthtwo: key.SequenceLengthTwo,
		Sequencedimone:    key.SequenceDimOne,
		Sequencedimtwo:    key.SequenceDimTwo,
	}
	return ret
}

func NewEuclideanPointKeyFromPoint(point kdtree.Point) *EuclideanPointKey {
	key := &EuclideanPointKey{
		Feature:           point.GetValues(),
		GroupLabel:        point.GetGroupLabel(),
		SequenceLengthOne: point.GetSequenceLengthOne(),
		SequenceLengthTwo: point.GetSequenceLengthTwo(),
		SequenceDimOne:    point.GetSequenceDimOne(),
		SequenceDimTwo:    point.GetSequenceDimTwo(),
	}
	return key
}

func NewEuclideanPointFromPoint(point kdtree.Point) *EuclideanPoint {
	ret := &EuclideanPoint{
		PointBase:         kdtree.NewPointBase(point.GetValues()),
		timestamp:         point.GetTimestamp(),
		label:             point.GetLabel(),
		groupLabel:        point.GetGroupLabel(),
		sequenceLengthOne: point.GetSequenceLengthOne(),
		sequenceLengthTwo: point.GetSequenceLengthTwo(),
		sequenceDimOne:    point.GetSequenceDimOne(),
		sequenceDimTwo:    point.GetSequenceDimTwo()}
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

func CalculateAverage(avg []float64, p []float64, n float64) []float64 {
	if n == 0 {
		return p
	}
	if len(avg) < len(p) {
		avg = make([]float64, len(p))
	}
	for i := 0; i < len(p); i++ {
		avg[i] += p[i] / n
	}
	return avg
}

// Insert entry into the data
func (dt *Data) Insert(key *EuclideanPointKey, value *EuclideanPointValue) error {
	// TODO: check for nil
	if len(key.Feature) == 0 {
		log.Printf("Data lenght is 0: %v\n", value.Label)
	}
	keyByte := EncodeEuclideanPointKey(key)
	valueByte := EncodeEuclideanPointValue(value)
	err := dt.DB.Update(func(txn *badger.Txn) error {
		err := txn.Set(keyByte, valueByte)
		return err
	})
	if err != nil {
		return err
	}
	dt.dirty = true
	dt.latestNumberOfChanges++
	return nil
}

func (dt *Data) InsertBasic(label string, vals ...float64) {
	d := int64(len(vals))
	key := EuclideanPointKey{
		Feature:           vals,
		SequenceLengthOne: 1,
		SequenceLengthTwo: 0,
		SequenceDimOne:    d,
		SequenceDimTwo:    0,
		GroupLabel:        label,
	}
	value := EuclideanPointValue{
		Timestamp:  0,
		Label:      label,
		GroupLabel: label,
	}
	dt.Insert(&key, &value)
}

func (dt *Data) Delete(key *EuclideanPointKey) error {
	keyByte := EncodeEuclideanPointKey(key)
	err := dt.DB.Update(func(txn *badger.Txn) error {
		err := txn.Delete(keyByte)
		return err
	})
	if err != nil {
		return err
	}
	dt.dirty = true
	dt.latestNumberOfChanges++
	return err
}

func (dt *Data) GetKnn(queryK int64, point *EuclideanPoint) ([]*EuclideanPoint, error) {
	// return dt.GetKnnLinear(queryK, point)
	if dt.tree != nil {
		dt.treeMu.RLock()
		ans := dt.tree.KNN(point, int(queryK))
		dt.treeMu.RUnlock()
		// size := len(point.GetValues())
		// fmt.Printf("Len ans: %v\n", len(ans))
		ret := make([]*EuclideanPoint, len(ans))
		for i := 0; i < len(ans); i++ {
			// fmt.Printf("Label: %v distance: %v\n", ans[i].GetLabel(), VectorDistance(point.GetValues(), ans[i].GetValues()))
			// fmt.Printf("Feature: %v\n", ans[i].GetValues())
			ret[i] = NewEuclideanPointFromPoint(ans[i])
		}
		return ret, nil
	}
	return []*EuclideanPoint{}, errors.New("Points not initialized yet")
}

func (dt *Data) GetKnnBasic(queryK int64, vals ...float64) ([]*EuclideanPoint, error) {
	point := NewEuclideanPointArr(vals)
	return dt.GetKnn(queryK, point)
}

func (dt *Data) Process(force bool) error {
	log.Printf("Data isEvictable %v\n", dt.IsEvictable)
	if dt.dirty || dt.latestNumberOfChanges > 0 || force {
		log.Printf("Running Process (forced: %v)\n", force)
		tempLatestNumberOfChanges := dt.latestNumberOfChanges
		dt.dirty = false
		points := make([]kdtree.Point, 0)
		n := int64(0)
		distance := 0.0
		maxDistance := 0.0
		avg := make([]float64, 0)
		hist := make([]float64, 64)
		nFloat := float64(dt.N)
		histUnit := 1 / nFloat
		averageTimeStamp := 0.0

		err := dt.DB.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				k := item.Key()
				err := item.Value(func(v []byte) error {
					euclideanPointKey := DecodeEuclideanPointKey(k)
					euclideanPointValue := DecodeEuclideanPointValue(v)
					// In eviction mode, if a point timestamp is older than average timestamp, delete data randomly.
					if dt.IsEvictable && dt.averageTimestamp != 0 && euclideanPointValue.Timestamp > dt.averageTimestamp && rand.Float32() < 0.2 {
						// dt.pointsMap.Delete(key)
						return nil // evict this data point from memory
					}
					point := NewEuclideanPointFromKeyValue(euclideanPointKey, euclideanPointValue)
					points = append(points, point)
					n++
					avg = CalculateAverage(avg, point.GetValues(), nFloat)
					averageTimeStamp = averageTimeStamp + float64(euclideanPointValue.Timestamp)/nFloat
					distance = VectorDistance(dt.Avg, point.GetValues())
					if distance > maxDistance {
						maxDistance = distance
					}
					if dt.MaxDistance != 0 {
						index := int((distance / dt.MaxDistance) * 64)
						if index >= 64 {
							index = 63
						}
						hist[index] += histUnit
					}
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		})

		if err != nil {
			return err
		}

		dt.Avg = avg
		dt.averageTimestamp = int64(averageTimeStamp)
		dt.Hist = hist
		dt.MaxDistance = maxDistance
		dt.N = n
		dt.timestamp = getCurrentTime()
		dt.latestNumberOfChanges = dt.latestNumberOfChanges - tempLatestNumberOfChanges

		if len(points) > 0 {
			tree := kdtree.NewKDTree(points)
			dt.treeMu.Lock()
			dt.tree = tree
			dt.treeMu.Unlock()
		}

	}
	dt.timestamp = getCurrentTime() // update always

	return nil
}

func (dt *Data) Run() error {
	nextTime := getCurrentTime()
	for {
		if nextTime <= getCurrentTime() {
			secondsToSleep := 10 + int64((dt.latestNumberOfChanges+1)%600)
			dt.Process(false)
			nextTime = getCurrentTime() + secondsToSleep
			dt.DB.RunValueLogGC(0.7)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)
		/*
			err := dt.DB.RunValueLogGC(0.7)
			if err != nil {
				og.Printf("DB Garbage Collection Error: %v\n", err)
			}
		*/
	}
	// return nil
}

const RefreshPeriod = 30 * time.Second

func (dt *Data) SetupRun() {
	log.Printf("Setup Run at %v Refresh period: %v\n", time.Now(), RefreshPeriod)
	dt.Process(false)
	ticker := time.NewTicker(RefreshPeriod)
	go func() {
		for {
			select {
			case <-ticker.C:
				dt.Process(false)
			}
		}
	}()
}

func (dt *Data) GetAll(stream pb.VeriService_GetLocalDataServer) error {
	err := dt.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				fmt.Printf("key=%s, value=%s\n", k, v)
				euclideanPointKey := DecodeEuclideanPointKey(k)
				euclideanPointValue := DecodeEuclideanPointValue(k)
				feature := FeatureFromEuclideanKeyValue(euclideanPointKey, euclideanPointValue)
				if streamErr := stream.Send(feature); streamErr != nil {
					return streamErr
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

// GetRandomPoints returns a sample set of data
func (dt *Data) GetRandomPoints(limit int) []kdtree.Point {
	count := 0
	points := make([]kdtree.Point, 0)
	err := dt.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if rand.Float64() < 0.5 {
				item := it.Item()
				k := item.Key()
				err := item.Value(func(v []byte) error {
					fmt.Printf("key=%s, value=%s\n", k, v)
					euclideanPointKey := DecodeEuclideanPointKey(k)
					euclideanPointValue := DecodeEuclideanPointValue(k)
					point := NewEuclideanPointFromKeyValue(euclideanPointKey, euclideanPointValue)
					points = append(points, point)
					return nil
				})
				if err != nil {
					return err
				}
				count++
				if count > limit {
					return nil
				}
			}
		}
		return nil
	})

	if err != nil {
		log.Printf("Error: %v\n", err)
	}
	return points
}

func (dt *Data) GetStats() *Stats {
	return &Stats{
		K:                dt.K,
		D:                dt.D,
		Avg:              dt.Avg,
		N:                dt.N,
		MaxDistance:      dt.MaxDistance,
		Hist:             dt.Hist,
		Timestamp:        dt.timestamp,
		AverageTimestamp: dt.averageTimestamp,
	}
}

type SortByDistance []*DataPoint

type DataPoint struct {
	Distance float64
	Point    *EuclideanPoint
}

func (e SortByDistance) Len() int {
	return len(e)
}
func (e SortByDistance) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}
func (e SortByDistance) Less(i, j int) bool {
	return e[i].Distance < e[j].Distance
}
