package data

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	bpb "github.com/dgraph-io/badger/v2/pb"
	"github.com/patrickmn/go-cache"
)

type SearchConfig struct {
	ScoreFuncName  string                                       `json:"scoreFuncName"`
	ScoreFunc      func(arr1 []float64, arr2 []float64) float64 `json:"-"`
	HigherIsBetter bool                                         `json:"higherIsBetter"`
	Limit          uint32                                       `json:"limit"`
	Duration       time.Duration                                `json:"-"`
}

func DefaultSearchConfig() *SearchConfig {
	return &SearchConfig{
		ScoreFuncName:  "VectorDistance",
		ScoreFunc:      VectorDistance,
		HigherIsBetter: false,
		Limit:          10,
		Duration:       1 * time.Second,
	}
}

func EncodeSearchConfig(p *SearchConfig) []byte {
	marshalled, _ := json.Marshal(p)
	return marshalled
}

// Collector collects results
type Collector struct {
	List           []*ScoredDatum
	ScoreFunc      func(arr1 []float64, arr2 []float64) float64
	MaxScore       float64
	DatumKey       *DatumKey
	N              uint32
	HigherIsBetter bool
}

// ScoredDatum helps to keep Data ordered
type ScoredDatum struct {
	Datum *Datum
	Score float64
}

// Insert add a new scored datum to collector
func (c *Collector) Insert(scoredDatum *ScoredDatum) error {
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

// Senc collects the results
func (c *Collector) Send(list *bpb.KVList) error {
	itemAdded := false
	for _, item := range list.Kv {
		datumKey, _ := ToDatumKey(item.Key)
		score := c.ScoreFunc(datumKey.Feature, c.DatumKey.Feature)
		if uint32(len(c.List)) < c.N {
			datum, _ := ToDatum(item.Key, item.Value)
			scoredDatum := &ScoredDatum{
				Datum: datum,
				Score: score,
			}
			c.List = append(c.List, scoredDatum)
			itemAdded = true
		} else if (c.HigherIsBetter && score > c.List[len(c.List)-1].Score) ||
			(!c.HigherIsBetter && score < c.List[len(c.List)-1].Score) {
			datum, _ := ToDatum(item.Key, item.Value)
			scoredDatum := &ScoredDatum{
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

// Search does a search based on distances of keys
func (dt *Data) Search(datum *Datum, config *SearchConfig) *Collector {
	if config == nil {
		config = DefaultSearchConfig()
	}
	c := &Collector{}
	c.DatumKey = datum.Key
	c.ScoreFunc = config.ScoreFunc
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
func (dt *Data) StreamSearch(datum *Datum, scoredDatumStream chan<- *ScoredDatum, queryWaitGroup *sync.WaitGroup, config *SearchConfig) error {
	collector := dt.Search(datum, config)
	for _, i := range collector.List {
		scoredDatumStream <- i
	}
	queryWaitGroup.Done()
	return nil
}

func GetSearchKey(datum *Datum, config *SearchConfig) string {
	keyByte, err := datum.GetKey()
	signature := EncodeSearchConfig(config)
	if err != nil {
		return string(signature)
	}
	return string(append(keyByte, signature...))
}

// SuperSearch searches and merges other resources
func (dt *Data) SuperSearch(datum *Datum, scoredDatumStreamOutput chan<- *ScoredDatum, config *SearchConfig) error {
	duration := config.Duration
	timeLimit := time.After(duration)
	queryKey := GetSearchKey(datum, config)
	if result, ok := dt.QueryCache.Get(queryKey); ok {
		cachedCollector := result.(*Collector)
		for _, i := range cachedCollector.List {
			scoredDatumStreamOutput <- i
		}
		return nil
	}
	// Search Start
	scoredDatumStream := make(chan *ScoredDatum, 100)
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
	temp, _ := NewTempData("...")
	defer temp.Close()
	dataAvailable := true
	for dataAvailable {
		select {
		case scoredDatum := <-scoredDatumStream:
			temp.Insert(scoredDatum.Datum, nil)
		case <-waitChannel:
			log.Printf("all data finished")
			close(scoredDatumStream)
			for scoredDatum := range scoredDatumStream {
				temp.Insert(scoredDatum.Datum, nil)
			}
			dataAvailable = false
			break
		case <-timeLimit:
			log.Printf("timeout")
			dataAvailable = false
			break
		}
	}
	// Search End
	collector := temp.Search(datum, config)
	for _, i := range collector.List {
		scoredDatumStreamOutput <- i
	}
	dt.QueryCache.Set(queryKey, collector, cache.DefaultExpiration)
	return nil
}
