package data

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/jinzhu/copier"

	"github.com/bgokden/veri/data/gencoder"
	pb "github.com/bgokden/veri/veriservice"
	badger "github.com/dgraph-io/badger/v2"
	bpb "github.com/dgraph-io/badger/v2/pb"
	"github.com/tidwall/gjson"
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
	log.Printf("SearchConfig Encoded: %v\n", string(marshalled))
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
	Filters        []string
	GroupFilters   []string
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
		datumScore := gencoder.DatumScore{Score: 0}
		datumScore.Unmarshal(item.UserMeta)
		// Old way of scoring:
		// datumKey, _ := ToDatumKey(item.Key)
		// // TODO: implement filters use: https://godoc.org/github.com/PaesslerAG/jsonpath#Get
		// score := c.ScoreFunc(datumKey.Feature, c.DatumKey.Feature)
		score := datumScore.Score
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

// ToList is a default implementation of KeyToList. It picks up all valid versions of the key,
// skipping over deleted or expired keys.
func (c *Collector) ToList(key []byte, itr *badger.Iterator) (*bpb.KVList, error) {
	list := &bpb.KVList{}
	for ; itr.Valid(); itr.Next() {
		item := itr.Item()
		if item.IsDeletedOrExpired() {
			break
		}
		if !bytes.Equal(key, item.Key()) {
			// Break out on the first encounter with another key.
			break
		}

		valCopy, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		keyCopy := item.KeyCopy(nil)
		datumKey, _ := ToDatumKey(keyCopy)
		datumScore := &gencoder.DatumScore{
			Score: c.ScoreFunc(datumKey.Feature, c.DatumKey.Feature),
		}

		if len(c.GroupFilters) > 0 {
			filterFailed := false
			jsonLabel := string(datumKey.GroupLabel)
			for _, filter := range c.Filters {
				value := gjson.Get(jsonLabel, filter)
				if !value.Exists() {
					filterFailed = true
					break
				}
			}
			if filterFailed {
				break
			}
		}

		if len(c.Filters) > 0 {
			filterFailed := false
			datumValue, _ := ToDatumValue(valCopy)
			jsonLabel := string(datumValue.Label)
			for _, filter := range c.Filters {
				value := gjson.Get(jsonLabel, filter)
				if !value.Exists() {
					filterFailed = true
					break
				}
			}
			if filterFailed {
				break
			}
		}

		datumScoreBytes, _ := datumScore.Marshal()
		kv := &bpb.KV{
			Key:       keyCopy,
			Value:     valCopy,
			UserMeta:  datumScoreBytes,
			Version:   item.Version(),
			ExpiresAt: item.ExpiresAt(),
		}
		list.Kv = append(list.Kv, kv)
		break
	}
	return list, nil
}

var vectorComparisonFuncs = map[string]func(arr1 []float64, arr2 []float64) float64{
	"VectorDistance":       VectorDistance,
	"VectorMultiplication": VectorMultiplication,
	"CosineSimilarity":     CosineSimilarity,
	"QuickVectorDistance":  QuickVectorDistance,
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
	c.List = make([]*pb.ScoredDatum, 0, config.Limit)
	c.DatumKey = datum.Key
	c.ScoreFunc = GetVectorComparisonFunction(config.ScoreFuncName)
	c.HigherIsBetter = config.HigherIsBetter
	c.N = config.Limit
	c.Filters = config.Filters
	c.GroupFilters = config.GroupFilters
	stream := dt.DB.NewStream()
	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = 4                      // Set number of goroutines to use for iteration.
	stream.Prefix = nil                   // Leave nil for iteration over the whole DB.
	stream.LogPrefix = "Badger.Streaming" // For identifying stream logs. Outputs to Logger.

	// ChooseKey is called concurrently for every key. If left nil, assumes true by default.
	stream.ChooseKey = nil

	// KeyToList is called concurrently for chosen keys. This can be used to convert
	// Badger data into custom key-values. If nil, uses stream.ToList, a default
	// implementation, which picks all valid key-values.
	stream.KeyToList = c.ToList // nil

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
func (dt *Data) StreamSearch(datum *pb.Datum, scoredDatumStream chan<- *pb.ScoredDatum, stopCh <-chan struct{}, queryWaitGroup *sync.WaitGroup, config *pb.SearchConfig) error {
	collector := dt.Search(datum, config)
	for _, i := range collector.List {
		// log.Printf("StreamSearch i: %v\n", i)
		select {
		case <-stopCh:
			break
		case scoredDatumStream <- i:
		}

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
func (dt *Data) AggregatedSearch(datum *pb.Datum, scoredDatumStreamOutput chan<- *pb.ScoredDatum, stopChOutput <-chan struct{}, upperWaitGroup *sync.WaitGroup, config *pb.SearchConfig) error {
	duration := time.Duration(config.Timeout) * time.Millisecond
	timeLimit := time.After(duration)
	queryKey := GetSearchKey(datum, config)
	if dt.QueryCache == nil {
		dt.InitData()
	}
	if result, ok := dt.QueryCache.Get(queryKey); ok {
		cachedResult := result.([]*pb.ScoredDatum)
		resultCopy := CloneResult(cachedResult)
		for _, i := range resultCopy {
			scoredDatumStreamOutput <- i
		}
		if upperWaitGroup != nil {
			upperWaitGroup.Done()
		}
		cacheDuration := time.Duration(config.CacheDuration) * time.Second
		dt.QueryCache.IncrementExpiration(queryKey, cacheDuration)
		return nil
	}
	// Search Start
	scoredDatumStream := make(chan *pb.ScoredDatum, 100)
	stopCh := make(chan struct{})
	var queryWaitGroup sync.WaitGroup
	waitChannel := make(chan struct{})
	go func() {
		defer close(waitChannel)
		queryWaitGroup.Wait()
	}()
	// internal
	queryWaitGroup.Add(1)
	go func() {
		dt.StreamSearch(datum, scoredDatumStream, stopCh, &queryWaitGroup, config)
	}()
	// external
	sourceList := dt.Sources.Items()
	for _, sourceItem := range sourceList {
		source := sourceItem.Object.(DataSource)
		queryWaitGroup.Add(1)
		go source.StreamSearch(datum, scoredDatumStream, stopCh, &queryWaitGroup, config)
	}
	// stream merge
	temp := NewAggrator(config, false, nil)
	dataAvailable := true
	for dataAvailable {
		select {
		case scoredDatum := <-scoredDatumStream:
			temp.Insert(scoredDatum)
		case <-waitChannel:
			log.Printf("AggregatedSearch: all data finished")
			// close(scoredDatumStream)
			close(stopCh)
			// for scoredDatum := range scoredDatumStream {
			// 	temp.Insert(scoredDatum)
			// }
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
	resultCopy := CloneResult(result)
	for _, i := range result {
		select {
		case <-stopChOutput:
			break
		case scoredDatumStreamOutput <- i:
		}
	}
	if upperWaitGroup != nil {
		upperWaitGroup.Done()
	}
	cacheDuration := time.Duration(config.CacheDuration) * time.Second
	dt.QueryCache.Set(queryKey, resultCopy, cacheDuration)
	log.Printf("AggregatedSearch: finished. Set Cache Duration: %v\n", cacheDuration)
	return nil
}

func CloneResult(result []*pb.ScoredDatum) []*pb.ScoredDatum {
	resultCopy := make([]*pb.ScoredDatum, len(result))
	for i, sd := range result {
		var scoredDatum pb.ScoredDatum
		copier.Copy(&scoredDatum, sd)
		resultCopy[i] = &scoredDatum
	}
	return resultCopy
}

// MultiAggregatedSearch searches and merges other resources
func (dt *Data) MultiAggregatedSearch(datumList []*pb.Datum, config *pb.SearchConfig, context *pb.SearchContext) ([]*pb.ScoredDatum, error) {
	duration := time.Duration(config.Timeout) * time.Millisecond
	timeLimit := time.After(duration)
	// Search Start
	scoredDatumStream := make(chan *pb.ScoredDatum, 100)
	stopCh := make(chan struct{})
	var queryWaitGroup sync.WaitGroup
	waitChannel := make(chan struct{})
	go func() {
		defer close(waitChannel)
		queryWaitGroup.Wait()
	}()
	// loop datumList
	for _, datum := range datumList {
		queryWaitGroup.Add(1)
		go dt.AggregatedSearch(datum, scoredDatumStream, stopCh, &queryWaitGroup, config)
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
			// close(scoredDatumStream)
			close(stopCh)
			// for scoredDatum := range scoredDatumStream {
			// 	temp.Insert(scoredDatum)
			// }
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
