package data

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	pb "github.com/bgokden/veri/veriservice"
	bpb "github.com/dgraph-io/badger/v2/pb"
	"github.com/patrickmn/go-cache"
)

// type SearchConfig struct {
// 	ScoreFuncName  string                                       `json:"scoreFuncName"`
// 	ScoreFunc      func(arr1 []float64, arr2 []float64) float64 `json:"-"`
// 	HigherIsBetter bool                                         `json:"higherIsBetter"`
// 	Limit          uint32                                       `json:"limit"`
// 	Duration       time.Duration                                `json:"-"`
// }

func DefaultSearchConfig() *pb.SearchConfig {
	return &pb.SearchConfig{
		ScoreFuncName: "VectorDistance",
		// ScoreFunc:      VectorDistance,
		HigherIsBetter: false,
		Limit:          10,
		Timeout:        1000, //milliseconds
	}
}

func EncodeSearchConfig(p *pb.SearchConfig) []byte {
	marshalled, _ := json.Marshal(p)
	return marshalled
}

// Collector collects results
type Collector struct {
	List           []*pb.ScoredDatum
	ScoreFunc      func(arr1 []float64, arr2 []float64) float64
	MaxScore       float64
	DatumKey       *pb.DatumKey
	N              uint32
	HigherIsBetter bool
}

// // ScoredDatum helps to keep Data ordered
// type ScoredDatum struct {
// 	Datum *Datum
// 	Score float64
// }

// Insert add a new scored datum to collector
func (c *Collector) Insert(scoredDatum *pb.ScoredDatum) error {
	itemAdded := false
	if uint32(len(c.List)) < c.N {
		c.List = append(c.List, scoredDatum)
		itemAdded = true
	} else if (c.HigherIsBetter && scoredDatum.Score > c.List[len(c.List)-1].Score) ||
		(!c.HigherIsBetter && scoredDatum.Score < c.List[len(c.List)-1].Score) {
		c.List[len(c.List)-1] = scoredDatum
		itemAdded = true
	}
	if itemAdded {
		if c.HigherIsBetter {
			sort.Slice(c.List, func(i, j int) bool {
				return c.List[i].Score > c.List[j].Score
			})
		} else {
			sort.Slice(c.List, func(i, j int) bool {
				return c.List[i].Score < c.List[j].Score
			})
		}
	}
	return nil
}

// Send collects the results
func (c *Collector) Send(list *bpb.KVList) error {
	// log.Printf("Collector Send\n")
	itemAdded := false
	for _, item := range list.Kv {
		datumKey, _ := ToDatumKey(item.Key)
		score := c.ScoreFunc(datumKey.Feature, c.DatumKey.Feature)
		if uint32(len(c.List)) < c.N {
			datum, _ := ToDatum(item.Key, item.Value)
			scoredDatum := &pb.ScoredDatum{
				Datum: datum,
				Score: score,
			}
			// log.Printf("ScoredDatum %v\n", scoredDatum)
			c.List = append(c.List, scoredDatum)
			itemAdded = true
		} else if (c.HigherIsBetter && score > c.List[len(c.List)-1].Score) ||
			(!c.HigherIsBetter && score < c.List[len(c.List)-1].Score) {
			datum, _ := ToDatum(item.Key, item.Value)
			scoredDatum := &pb.ScoredDatum{
				Datum: datum,
				Score: score,
			}
			c.List[len(c.List)-1] = scoredDatum
			itemAdded = true
		}
		if itemAdded {
			if c.HigherIsBetter {
				sort.Slice(c.List, func(i, j int) bool {
					return c.List[i].Score > c.List[j].Score
				})
			} else {
				sort.Slice(c.List, func(i, j int) bool {
					return c.List[i].Score < c.List[j].Score
				})
			}
			itemAdded = false
		}
	}
	return nil
}

var vectorComparisonFuncs = map[string]func(arr1 []float64, arr2 []float64) float64{
	"VectorDistance":       VectorDistance,
	"VectorMultiplication": VectorMultiplication,
	"CosineSimilarity":     CosineSimilarity,
}

func GetVectorComparisonFunction(name string) func(arr1 []float64, arr2 []float64) float64 {
	if function, ok := vectorComparisonFuncs[name]; ok {
		return function
	}
	return VectorDistance
}

// Search does a search based on distances of keys
func (dt *Data) Search(datum *pb.Datum, config *pb.SearchConfig) *Collector {
	// log.Printf("Search: %v\n", datum)
	if config == nil {
		config = DefaultSearchConfig()
	}
	c := &Collector{}
	c.DatumKey = datum.Key
	c.ScoreFunc = GetVectorComparisonFunction(config.ScoreFuncName)
	c.HigherIsBetter = config.HigherIsBetter
	c.N = config.Limit
	stream := dt.DB.NewStream()
	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = 16                     // Set number of goroutines to use for iteration.
	stream.Prefix = nil                   // Leave nil for iteration over the whole DB.
	stream.LogPrefix = "Badger.Streaming" // For identifying stream logs. Outputs to Logger.

	// ChooseKey is called concurrently for every key. If left nil, assumes true by default.
	stream.ChooseKey = nil

	// KeyToList is called concurrently for chosen keys. This can be used to convert
	// Badger data into custom key-values. If nil, uses stream.ToList, a default
	// implementation, which picks all valid key-values.
	stream.KeyToList = nil

	// -- End of optional settings.

	// Send is called serially, while Stream.Orchestrate is running.

	stream.Send = c.Send

	// Run the stream
	if err := stream.Orchestrate(context.Background()); err != nil {
		return nil
	}
	// Done.
	return c
}

// StreamSearch does a search based on distances of keys
func (dt *Data) StreamSearch(datum *pb.Datum, scoredDatumStream chan<- *pb.ScoredDatum, queryWaitGroup *sync.WaitGroup, config *pb.SearchConfig) error {
	collector := dt.Search(datum, config)
	for _, i := range collector.List {
		// log.Printf("StreamSearch i: %v\n", i)
		scoredDatumStream <- i
	}
	queryWaitGroup.Done()
	return nil
}

func GetSearchKey(datum *pb.Datum, config *pb.SearchConfig) string {
	keyByte, err := GetKeyAsBytes(datum)
	signature := EncodeSearchConfig(config)
	if err != nil {
		return string(signature)
	}
	return string(append(keyByte, signature...))
}

// AggregatedSearch searches and merges other resources
func (dt *Data) AggregatedSearch(datum *pb.Datum, scoredDatumStreamOutput chan<- *pb.ScoredDatum, upperWaitGroup *sync.WaitGroup, config *pb.SearchConfig) error {
	duration := time.Duration(config.Timeout) * time.Millisecond
	timeLimit := time.After(duration)
	// log.Printf("DatumKey: %v\n", datum.GetKey())
	queryKey := GetSearchKey(datum, config)
	if result, ok := dt.QueryCache.Get(queryKey); ok {
		cachedResult := result.([]*pb.ScoredDatum)
		for _, i := range cachedResult {
			scoredDatumStreamOutput <- i
		}
		return nil
	}
	// Search Start
	scoredDatumStream := make(chan *pb.ScoredDatum, 100)
	var queryWaitGroup sync.WaitGroup
	waitChannel := make(chan struct{})
	go func() {
		defer close(waitChannel)
		queryWaitGroup.Wait()
	}()
	// internal
	queryWaitGroup.Add(1)
	go func() {
		dt.StreamSearch(datum, scoredDatumStream, &queryWaitGroup, config)
	}()
	// external
	sourceList := dt.Sources.Items()
	for _, sourceItem := range sourceList {
		source := sourceItem.Object.(DataSource)
		queryWaitGroup.Add(1)
		go source.StreamSearch(datum, scoredDatumStream, &queryWaitGroup, config)
	}
	// stream merge
	isGrouped := false
	if config.GroupLimit > 0 {
		isGrouped = true
	}
	temp := NewAggrator(config, isGrouped, nil)
	dataAvailable := true
	for dataAvailable {
		select {
		case scoredDatum := <-scoredDatumStream:
			temp.Insert(scoredDatum)
		case <-waitChannel:
			log.Printf("AggregatedSearch: all data finished")
			close(scoredDatumStream)
			for scoredDatum := range scoredDatumStream {
				temp.Insert(scoredDatum)
			}
			dataAvailable = false
			break
		case <-timeLimit:
			log.Printf("timeout")
			dataAvailable = false
			break
		}
	}
	log.Printf("search collected data\n")
	// Search End
	result := temp.Result()
	for _, i := range result {
		scoredDatumStreamOutput <- i
	}
	if upperWaitGroup != nil {
		upperWaitGroup.Done()
	}
	dt.QueryCache.Set(queryKey, result, cache.DefaultExpiration)
	log.Printf("AggregatedSearch: finished")
	return nil
}

// MultiAggregatedSearch searches and merges other resources
func (dt *Data) MultiAggregatedSearch(datumList []*pb.Datum, config *pb.SearchConfig, context *pb.SearchContext) ([]*pb.ScoredDatum, error) {
	duration := time.Duration(config.Timeout) * time.Millisecond
	timeLimit := time.After(duration)
	// Search Start
	scoredDatumStream := make(chan *pb.ScoredDatum, 100)
	var queryWaitGroup sync.WaitGroup
	waitChannel := make(chan struct{})
	go func() {
		defer close(waitChannel)
		queryWaitGroup.Wait()
	}()
	// loop datumList
	for _, datum := range datumList {
		queryWaitGroup.Add(1)
		go dt.AggregatedSearch(datum, scoredDatumStream, &queryWaitGroup, config)
	}
	// stream merge
	isGrouped := false
	if config.GroupLimit > 0 {
		isGrouped = true
	}
	temp := NewAggrator(config, isGrouped, context)
	dataAvailable := true
	for dataAvailable {
		select {
		case scoredDatum := <-scoredDatumStream:
			temp.Insert(scoredDatum)
		case <-waitChannel:
			log.Printf("MultiAggregatedSearch: all data finished")
			close(scoredDatumStream)
			for scoredDatum := range scoredDatumStream {
				temp.Insert(scoredDatum)
			}
			dataAvailable = false
			break
		case <-timeLimit:
			log.Printf("timeout")
			dataAvailable = false
			break
		}
	}
	log.Printf("search collected data\n")
	// Search End
	log.Printf("MultiAggregatedSearch: finished")
	return temp.Result(), nil
}
