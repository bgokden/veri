package data

import (
	"encoding/json"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jinzhu/copier"

	"github.com/bgokden/veri/util"
	pb "github.com/bgokden/veri/veriservice"
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

// StreamSearch does a search based on distances of keys
func (dt *Data) StreamSearch(datum *pb.Datum, scoredDatumStream chan<- *pb.ScoredDatum, queryWaitGroup *sync.WaitGroup, config *pb.SearchConfig) error {
	var collector *Collector
	defer queryWaitGroup.Done()
	if config == nil {
		config = DefaultSearchConfig()
	}
	if strings.HasPrefix(config.ScoreFuncName, "Annoy") {
		collector = dt.SearchAnnoy(datum, config)
	} else {
		// return errors.New("Base search is not working yet")
		collector = dt.Search(datum, config, nil)
	}
	if collector != nil {
		for _, i := range collector.List {
			// log.Printf("StreamSearch i: %v\n", i)
			scoredDatumStream <- i
		}
	}
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
		// log.Printf("Query Datasource: %v\n", source.GetID())
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
			log.Printf("timeout")
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
				}
			}
		}
		dt.Annoyer.RUnlock()
	} else {
		log.Println("Fallback to regular search")
		// return c // Fallback is emptry list now
		return dt.Search(datum, config, nil)
	}
	return c
}

// Search does a search based on distances of keys
func (dt *Data) Search(datum *pb.Datum, config *pb.SearchConfig, c *Collector) *Collector {
	// log.Printf("Search: %v\n", datum)
	if c == nil {
		if config == nil {
			config = DefaultSearchConfig()
		}
		c = &Collector{}
		c.List = make([]*pb.ScoredDatum, 0, config.Limit)
		c.DatumKey = datum.Key
		c.ScoreFunc = GetVectorComparisonFunction(config.ScoreFuncName)
		c.HigherIsBetter = config.HigherIsBetter
		c.N = config.Limit
		c.Filters = config.Filters
		c.GroupFilters = config.GroupFilters
	}

	dt.LoopDBMap(func(entry *DBMapEntry) error {
		if c.PassesFilters(entry.Datum) {
			err := c.Insert(&pb.ScoredDatum{
				Score: c.ScoreFunc(entry.Datum.Key.Feature, datum.Key.Feature),
				Datum: entry.Datum,
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return c
}
