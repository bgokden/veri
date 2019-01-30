package data

import (
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	kdtree "github.com/bgokden/go-kdtree"
	pb "github.com/bgokden/veri/veriservice"
	"github.com/gaspiman/cosine_similarity"
)

// This is set in compile time for optimization
const k = 1024 // 1024

// 0 => euclidean distance
// 1 => consine distance
const distance_mode = 0

type Data struct {
	k                     int64
	d                     int64
	avg                   []float64
	n                     int64
	maxDistance           float64
	hist                  []float64
	timestamp             int64
	averageTimestamp      int64
	dirty                 bool
	latestNumberOfChanges int
	pointsMap             sync.Map
	pointsMu              sync.RWMutex // protects points
	treeMu                sync.RWMutex // protects KDTree
	tree                  *kdtree.KDTree
	isEvictable           bool
}

func NewData() *Data {
	dt := Data{}
	go dt.Run()
	return &dt
}

type Stats struct {
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
	Feature           [k]float64
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

func euclideanDistance(arr1 []float64, arr2 []float64) float64 {
	var ret float64
	for i := 0; i < len(arr1); i++ {
		tmp := arr1[i] - arr2[i]
		ret += tmp * tmp
	}
	// fmt.Printf("%v\n", ret)
	return ret
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

func VectorDistance(arr1 []float64, arr2 []float64) float64 {
	minLen := min(len(arr1), len(arr2))
	return euclideanDistance(arr1[:minLen], arr2[:minLen])
	/*
		if distance_mode == 1 {
			return cosineDistance(arr1, arr2)
		} else {
			return euclideanDistance(arr1, arr2)
		}
	*/
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

func (dt *Data) Insert(key EuclideanPointKey, value EuclideanPointValue) {
	dt.pointsMap.Store(key, value)
	dt.dirty = true
	dt.latestNumberOfChanges++
}

func (dt *Data) InsertBasic(label string, vals ...float64) {
	d := int64(len(vals))
	if dt.d < d {
		if d > k {
			d = k // d can not be larger than maximum capacity
		}
		log.Printf("Updating current dimension to: %v", d)
		dt.d = d // Maybe we can use max of
	}
	key := EuclideanPointKey{
		SequenceLengthOne: 1,
		SequenceLengthTwo: 0,
		SequenceDimOne:    d,
		SequenceDimTwo:    0,
		GroupLabel:        label,
	}
	copy(key.Feature[:d], vals[:d])
	value := EuclideanPointValue{
		Timestamp:  0,
		Label:      label,
		GroupLabel: label,
	}
	dt.Insert(key, value)
}

func (dt *Data) Delete(key EuclideanPointKey) {
	dt.pointsMap.Delete(key)
	dt.dirty = true
	dt.latestNumberOfChanges++
}

func (dt *Data) GetKnn(queryK int64, point *EuclideanPoint) ([]*EuclideanPoint, error) {
	if dt.tree != nil {
		dt.treeMu.RLock()
		ans := dt.tree.KNN(point, int(queryK))
		dt.treeMu.RUnlock()
		// fmt.Printf("Len ans: %v\n", len(ans))
		ret := make([]*EuclideanPoint, len(ans))
		for i := 0; i < len(ans); i++ {
			// fmt.Printf("Label: %v distance: %v\n", ans[i].GetLabel(), VectorDistance(point.GetValues()[:size], ans[i].GetValues()[:size]))
			// fmt.Printf("Feature: %v\n", ans[i].GetValues()[:size])
			ret[i] = &EuclideanPoint{
				PointBase:         kdtree.NewPointBase(ans[i].GetValues()[:dt.d]),
				timestamp:         ans[i].GetTimestamp(),
				label:             ans[i].GetLabel(),
				groupLabel:        ans[i].GetGroupLabel(),
				sequenceLengthOne: ans[i].GetSequenceLengthOne(),
				sequenceLengthTwo: ans[i].GetSequenceLengthTwo(),
				sequenceDimOne:    ans[i].GetSequenceDimOne(),
				sequenceDimTwo:    ans[i].GetSequenceDimTwo()}
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
	if dt.dirty || dt.isEvictable || dt.latestNumberOfChanges > 0 || force {
		fmt.Printf("Running Process\n")
		tempLatestNumberOfChanges := dt.latestNumberOfChanges
		dt.dirty = false
		points := make([]kdtree.Point, 0)
		n := int64(0)
		distance := 0.0
		maxDistance := 0.0
		avg := make([]float64, 0)
		hist := make([]float64, 64)
		nFloat := float64(dt.n)
		histUnit := 1 / nFloat
		averageTimeStamp := 0.0
		dt.pointsMap.Range(func(key, value interface{}) bool {
			euclideanPointKey := key.(EuclideanPointKey)
			euclideanPointValue := value.(EuclideanPointValue)
			// In eviction mode, if a point timestamp is older than average timestamp, delete data randomly.
			if dt.isEvictable && dt.averageTimestamp != 0 && euclideanPointValue.Timestamp > dt.averageTimestamp && rand.Float32() < 0.2 {
				dt.pointsMap.Delete(key)
				return true // evict this data point
			}
			point := NewEuclideanPointArrWithLabel(
				euclideanPointKey.Feature,
				euclideanPointValue.Timestamp,
				euclideanPointValue.Label,
				euclideanPointValue.GroupLabel,
				dt.d,
				euclideanPointKey.SequenceLengthOne,
				euclideanPointKey.SequenceLengthTwo,
				euclideanPointKey.SequenceDimOne,
				euclideanPointKey.SequenceDimTwo)
			points = append(points, point)
			n++
			avg = CalculateAverage(avg, point.GetValues(), nFloat)
			averageTimeStamp = averageTimeStamp + float64(euclideanPointValue.Timestamp)/nFloat
			distance = VectorDistance(dt.avg, point.GetValues())
			if distance > maxDistance {
				maxDistance = distance
			}
			if dt.maxDistance != 0 {
				index := int((distance / dt.maxDistance) * 64)
				if index >= 64 {
					index = 63
				}
				hist[index] += histUnit
			}
			return true
		})
		dt.avg = avg
		dt.averageTimestamp = int64(averageTimeStamp)
		dt.hist = hist
		dt.maxDistance = maxDistance
		dt.n = n
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
			secondsToSleep := 3 + int64((dt.latestNumberOfChanges+1)%60)
			dt.Process(false)
			nextTime = getCurrentTime() + secondsToSleep
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)
	}
	return nil
}

func (dt *Data) GetAll(stream pb.VeriService_GetLocalDataServer) error {
	dt.pointsMap.Range(func(key, value interface{}) bool {
		euclideanPointKey := key.(EuclideanPointKey)
		euclideanPointValue := value.(EuclideanPointValue)
		feature := &pb.Feature{
			Feature:    euclideanPointKey.Feature[:dt.d],
			Timestamp:  euclideanPointValue.Timestamp,
			Label:      euclideanPointValue.Label,
			Grouplabel: euclideanPointValue.GroupLabel,
		}
		if err := stream.Send(feature); err != nil {
			// return err pass err someway
			return false
		}
		return true
	})

	return nil
}

func (dt *Data) GetRandomPoints(limit int) []kdtree.Point {
	count := 0
	points := make([]kdtree.Point, 0)
	dt.pointsMap.Range(func(key, value interface{}) bool {
		euclideanPointKey := key.(EuclideanPointKey)
		euclideanPointValue := value.(EuclideanPointValue)
		point := NewEuclideanPointArrWithLabel(
			euclideanPointKey.Feature,
			euclideanPointValue.Timestamp,
			euclideanPointValue.Label,
			euclideanPointValue.GroupLabel,
			dt.d,
			euclideanPointKey.SequenceLengthOne,
			euclideanPointKey.SequenceLengthTwo,
			euclideanPointKey.SequenceDimOne,
			euclideanPointKey.SequenceDimTwo)
		if rand.Float64() < 0.5 { // TODO: improve randomness
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
	return points
}
