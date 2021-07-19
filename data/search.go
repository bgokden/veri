package data

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jinzhu/copier"

	"github.com/bgokden/veri/data/gencoder"
	"github.com/bgokden/veri/util"
	pb "github.com/bgokden/veri/veriservice"
	badger "github.com/dgraph-io/badger/v3"
	bpb "github.com/dgraph-io/badger/v3/pb"
	pbp "github.com/dgraph-io/badger/v3/pb"
	"github.com/dgraph-io/ristretto/z"
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

func EncodeSearchConfig(sc *pb.SearchConfig) []byte {
	var config pb.SearchConfig
	copier.Copy(&config, sc)
	config.Uuid = ""
	marshalled, _ := json.Marshal(&config)
	// log.Printf("SearchConfig Encoded: %v\n", string(marshalled))
	return marshalled
}

// Collector collects results
type Collector struct {
	List           []*pb.ScoredDatum
	ScoreFunc      func(arr1 []float32, arr2 []float32) float64
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
func (c *Collector) Send(buf *z.Buffer) error {
	err := buf.SliceIterate(func(s []byte) error {
		kv := new(pbp.KV)
		if err := kv.Unmarshal(s); err != nil {
			return err
		}
		if kv.StreamDone == true {
			return nil
		}
		/////
		datumScore := gencoder.DatumScore{Score: 0}
		datumScore.Unmarshal(kv.UserMeta)
		// Old way of scoring:
		// datumKey, _ := ToDatumKey(item.Key)
		// // TODO: implement filters use: https://godoc.org/github.com/PaesslerAG/jsonpath#Get
		// score := c.ScoreFunc(datumKey.Feature, c.DatumKey.Feature)
		score := datumScore.Score
		itemAdded := false
		if uint32(len(c.List)) < c.N {
			datum, _ := ToDatum(kv.Key, kv.Value)
			scoredDatum := &pb.ScoredDatum{
				Datum: datum,
				Score: score,
			}
			// log.Printf("ScoredDatum %v\n", scoredDatum)
			c.List = append(c.List, scoredDatum)
			itemAdded = true
		} else if (c.HigherIsBetter && score > c.List[len(c.List)-1].Score) ||
			(!c.HigherIsBetter && score < c.List[len(c.List)-1].Score) {
			datum, _ := ToDatum(kv.Key, kv.Value)
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
		}
		/////
		return nil
	})
	return err
}

func (c *Collector) PassesFilters(datum *pb.Datum) bool {
	if len(c.GroupFilters) > 0 {
		jsonLabel := string(datum.Key.GroupLabel)
		for _, filter := range c.GroupFilters {
			value := gjson.Get(jsonLabel, filter)
			if !value.Exists() {
				return false
			}
		}
	}

	if len(c.Filters) > 0 {
		jsonLabel := string(datum.Value.Label)
		for _, filter := range c.Filters {
			value := gjson.Get(jsonLabel, filter)
			if !value.Exists() {
				return false
			}
		}
	}
	return true
}

// ToList is a default implementation of KeyToList. It picks up all valid versions of the key,
// skipping over deleted or expired keys.
// TODO: update to bagder/v3 allocators
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
			for _, filter := range c.GroupFilters {
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

var vectorComparisonFuncs = map[string]func(arr1 []float32, arr2 []float32) float64{
	"AnnoyVectorDistance":   VectorDistance,
	"AnnoyCosineSimilarity": CosineSimilarity,
	"VectorDistance":        VectorDistance,
	"VectorMultiplication":  VectorMultiplication,
	"CosineSimilarity":      CosineSimilarity,
	"QuickVectorDistance":   QuickVectorDistance,
	"AngularDistance":       AngularDistance,
	"AnnoyAngularDistance":  AngularDistance,
}

func GetVectorComparisonFunction(name string) func(arr1 []float32, arr2 []float32) float64 {
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
	stream.NumGo = 16                     // Set number of goroutines to use for iteration.
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
func (dt *Data) StreamSearch(datum *pb.Datum, scoredDatumStream chan<- *pb.ScoredDatum, queryWaitGroup *sync.WaitGroup, config *pb.SearchConfig) error {
	var collector *Collector
	if config == nil {
		config = DefaultSearchConfig()
	}
	if strings.HasPrefix(config.ScoreFuncName, "Annoy") {
		collector = dt.SearchAnnoy(datum, config)
	} else {
		collector = dt.Search(datum, config)
	}
	if collector != nil {
		for _, i := range collector.List {
			// log.Printf("StreamSearch i: %v\n", i)
			scoredDatumStream <- i
		}
	}
	queryWaitGroup.Done()
	return nil
}

func GetSearchKey(datum *pb.Datum, config *pb.SearchConfig) string {
	keyByte, err := GetKeyAsBytes(datum)
	signature := EncodeSearchConfig(config)
	if err != nil {
		return util.EncodeToString(signature)
	}
	return util.EncodeToString(append(keyByte, signature...))
}

// AggregatedSearch searches and merges other resources
func (dt *Data) AggregatedSearch(datum *pb.Datum, scoredDatumStreamOutput chan<- *pb.ScoredDatum, upperWaitGroup *sync.WaitGroup, config *pb.SearchConfig) error {
	duration := time.Duration(config.Timeout) * time.Millisecond
	timeLimit := time.After(duration)
	queryKey := GetSearchKey(datum, config)
	config.Timeout = uint64(float64(config.Timeout) * 0.9) // Decrase timeout for downstream
	if dt.QueryCache == nil {
		dt.InitData()
	}
	if config.CacheDuration > 0 {
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
	}
	// Search Start
	scoredDatumStream := make(chan *pb.ScoredDatum, 100)
	var queryWaitGroup sync.WaitGroup
	waitChannel := make(chan struct{})
	// internal
	queryWaitGroup.Add(1)
	go func() {
		dt.StreamSearch(datum, scoredDatumStream, &queryWaitGroup, config)
	}()
	// external
	dt.RunOnRandomSources(5, func(source DataSource) error {
		queryWaitGroup.Add(1)
		go source.StreamSearch(datum, scoredDatumStream, &queryWaitGroup, config)
		return nil
	})
	go func() {
		defer close(waitChannel)
		queryWaitGroup.Wait()
	}()
	// stream merge
	temp := NewAggrator(config, false, nil)
	dataAvailable := true
	for dataAvailable {
		select {
		case scoredDatum := <-scoredDatumStream:
			temp.Insert(scoredDatum)
		case <-waitChannel:
			// log.Printf("AggregatedSearch: all data finished")
			close(scoredDatumStream)
			for scoredDatum := range scoredDatumStream {
				temp.Insert(scoredDatum)
			}
			dataAvailable = false
			break
		case <-timeLimit:
			// log.Printf("timeout")
			dataAvailable = false
			break
		}
	}
	// log.Printf("search collected data\n")
	// Search End
	result := temp.Result()
	resultCopy := CloneResult(result)
	for _, i := range result {
		scoredDatumStreamOutput <- i
	}
	if upperWaitGroup != nil {
		upperWaitGroup.Done()
	}
	if config.CacheDuration > 0 {
		cacheDuration := time.Duration(config.CacheDuration) * time.Second
		dt.QueryCache.Set(queryKey, resultCopy, cacheDuration)
		// log.Printf("AggregatedSearch: finished. Set Cache Duration: %v\n", cacheDuration)
	}
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
	var queryWaitGroup sync.WaitGroup
	waitChannel := make(chan struct{})
	// loop datumList
	for _, datum := range datumList {
		queryWaitGroup.Add(1)
		go dt.AggregatedSearch(datum, scoredDatumStream, &queryWaitGroup, config)
	}
	go func() {
		defer close(waitChannel)
		queryWaitGroup.Wait()
	}()
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
			// log.Printf("MultiAggregatedSearch: all data finished")
			close(scoredDatumStream)
			for scoredDatum := range scoredDatumStream {
				temp.Insert(scoredDatum)
			}
			dataAvailable = false
			break
		case <-timeLimit:
			// log.Printf("timeout")
			dataAvailable = false
			break
		}
	}
	// Search End
	// log.Printf("MultiAggregatedSearch: finished")
	return temp.Result(), nil
}

// Search does a search based on distances of keys
func (dt *Data) SearchAnnoy(datum *pb.Datum, config *pb.SearchConfig) *Collector {
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
	// features32 := make([]float32, len(datum.Key.Feature))
	// for i, f := range datum.Key.Feature {
	// 	features32[i] = float32(f)
	// }
	if dt.Annoyer.AnnoyIndex != nil && dt.Annoyer.DataIndex != nil && len(*(dt.Annoyer.DataIndex)) > 0 {
		dt.Annoyer.RLock()
		var result []int
		var distances []float32
		index := *(dt.Annoyer.DataIndex)
		dt.Annoyer.AnnoyIndex.GetNnsByVector(datum.Key.Feature, len(index), int(config.Limit), &result, &distances)
		if result != nil {
			counter := uint32(0)
			for i := 0; i < len(result); i++ {
				datumE := index[result[i]]
				if datumE != nil && c.PassesFilters(datumE) {
					scoredDatum := &pb.ScoredDatum{
						Datum: datumE,
						Score: c.ScoreFunc(datum.Key.Feature, datumE.Key.Feature),
					}
					// log.Printf("Result %v d: %v\n", result[i], distances[i])
					c.List = append(c.List, scoredDatum)
					counter += 1
					if counter >= c.N {
						break
					}
				} else {
					log.Printf("Datum E is nil. %v d: %v\n", result[i], distances[i])
				}
			}
		}
		dt.Annoyer.RUnlock()
	} else {
		log.Println("Fallback to regular search")
		return dt.Search(datum, config)
	}
	return c
}
