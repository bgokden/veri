package data

import (
	"errors"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/bgokden/go-cache"
	badger "github.com/dgraph-io/badger/v3"

	"github.com/bgokden/veri/annoyindex"

	pb "github.com/bgokden/veri/veriservice"
)

type DataSource interface {
	StreamSearch(datumList *pb.Datum, scoredDatumStream chan<- *pb.ScoredDatum, queryWaitGroup *sync.WaitGroup, config *pb.SearchConfig) error
	Insert(datum *pb.Datum, config *pb.InsertConfig) error
	GetDataInfo() *pb.DataInfo
	GetID() string
}

type Annoyer struct {
	sync.RWMutex
	DataIndex  *[]*pb.Datum
	AnnoyIndex annoyindex.AnnoyIndex
}

// Data represents a dataset with similar struture
type Data struct {
	Config      *pb.DataConfig
	Path        string
	Avg         []float32
	N           uint64
	MaxDistance float64
	Hist        []float32
	Timestamp   uint64
	DB          *badger.DB
	DBPath      string
	Dirty       bool
	Sources     *cache.Cache
	QueryCache  *cache.Cache
	Initialized bool
	Alive       bool
	Annoyer     Annoyer
}

func (d *Data) GetConfig() *pb.DataConfig {
	return d.Config
}

// NewData creates a data struct
func NewData(config *pb.DataConfig, dataPath string) (*Data, error) {
	dt := &Data{
		Config: config,
	}
	// log.Printf("Create Data\n")
	dt.DBPath = path.Join(dataPath, config.Name)
	dt.InitData()
	return dt, nil
}

// NewPreData creates a data struct
func NewPreData(config *pb.DataConfig, dataPath string) *Data {
	dt := &Data{
		Config: config,
	}
	// log.Printf("Pre Create Data %v\n", dt.Config)
	dt.DBPath = path.Join(dataPath, config.Name)
	return dt
}

func (dt *Data) InitData() error {
	log.Printf("Init Data %v\n", dt.Config)
	if dt.Initialized == false {
		options := badger.DefaultOptions(dt.DBPath).
			WithLoggingLevel(badger.WARNING)
		db, err := badger.Open(options)
		if err != nil {
			return err
		}
		dt.DB = db
		dt.Sources = cache.New(5*time.Minute, 1*time.Minute)
		dt.QueryCache = cache.New(5*time.Minute, 1*time.Minute)
		dt.Alive = true
		go dt.Run()
		dt.Initialized = true
	}
	return nil
}

// NewTempData return an inmemory badger instance
func NewTempData() (*Data, error) {
	dt := &Data{
		Config: &pb.DataConfig{
			NoTarget: true,
		},
	}
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		return nil, err
	}
	dt.DB = db
	return dt, nil
}

// Close currently closes underlying kv store
func (dt *Data) Close() error {
	dt.Alive = false
	return dt.DB.Close()
}

// Delete currently deletes underlying data folder ignores errors.
func (dt *Data) DeletePath() error {
	dt.DB.Close()
	os.RemoveAll(dt.DBPath)
	return nil
}

// Run runs statistical calculation regularly
func (dt *Data) Run() error {
	nextTime := getCurrentTime()
	for {
		if !dt.Alive {
			break
		}
		if nextTime <= getCurrentTime() {
			secondsToSleep := uint64(10) // increment this based on load
			dt.Process(false)
			nextTime = getCurrentTime() + secondsToSleep
			dt.DB.RunValueLogGC(0.7)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

	}
	return nil
}

// Process runs through keys and calculates statistics
func (dt *Data) Process(force bool) error {
	// log.Printf("Try Running Process (forced: %v) current: %v timestamp: %v diff: %v\n", force, getCurrentTime(), dt.Timestamp, getCurrentTime()-dt.Timestamp)
	if getCurrentTime()-dt.Timestamp >= 60 || force {
		// log.Printf("Running Process (forced: %v)\n", force)
		n := uint64(0)
		distance := 0.0
		maxDistance := 0.0
		avg := make([]float32, 0)
		hist := make([]float32, 64)
		nFloat := float32(dt.N)
		if nFloat == 0 {
			// log.Printf("Data size was 0\n")
			nFloat = 1
		}
		histUnit := 1 / nFloat
		newDataIndex := make([]*pb.Datum, max(1000, int(dt.N)))
		var newAnnoyIndex annoyindex.AnnoyIndex
		err := dt.DB.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				k := item.Key()
				datumKey, err := ToDatumKey(k)
				if err == nil {
					n++
					avg = CalculateAverage(avg, datumKey.Feature, nFloat)
					distance = VectorDistance(dt.Avg, datumKey.Feature)

					if distance > maxDistance {
						maxDistance = distance
					}
					if dt.MaxDistance != 0 {
						index := int((distance / dt.MaxDistance) * 64)
						if index >= 64 {
							index = 63
						}
						if index <= 0 {
							index = 0 // this is probably related to a bug
						}
						hist[index] += histUnit
					}
				}
				err = item.Value(func(v []byte) error {
					datum, errV := ToDatum(k, v)
					if errV == nil {
						// features32 := make([]float32, len(datum.Key.Feature))
						// for i, f := range datum.Key.Feature {
						// 	features32[i] = float32(f)
						// }
						i := int(n - 1)
						if i < len(newDataIndex) {
							if newAnnoyIndex == nil {
								newAnnoyIndex = annoyindex.NewAnnoyIndexEuclidean(len(datum.Key.Feature))
							}
							newAnnoyIndex.AddItem(i, datum.Key.Feature)
							newDataIndex[i] = datum
						}
						// newDataIndex = append(newDataIndex, datum)
					}
					return nil
				})
				if err != nil {
					log.Printf("err: %v\n", err)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		dt.Avg = avg
		dt.Hist = hist
		dt.MaxDistance = maxDistance
		dt.N = n
		dt.Timestamp = getCurrentTime()
		if newAnnoyIndex != nil {
			newAnnoyIndex.Build(10)
			// log.Printf("Updating index. len: %v\n", len(newDataIndex))
			dt.Annoyer.Lock()
			dt.Annoyer.AnnoyIndex = newAnnoyIndex
			dt.Annoyer.DataIndex = &newDataIndex
			dt.Annoyer.Unlock()
			// log.Printf("Updated index\n")
		}
		// if dt.ActiveIndex == 0 {
		// 	dt.AnnoyIndexB = newAnnoyIndex
		// 	dt.IndexB = &newDataIndex
		// 	dt.ActiveIndex = 1
		// } else {
		// 	dt.AnnoyIndexA = newAnnoyIndex
		// 	dt.IndexA = &newDataIndex
		// 	dt.ActiveIndex = 0
		// }

		dt.SyncAll()
	}
	// dt.Timestamp = getCurrentTime() // update always
	dt.Dirty = false
	return nil
}

// GetDataInfo out of data
func (dt *Data) GetDataInfo() *pb.DataInfo {
	// log.Printf("Data: %v\n", dt)
	return &pb.DataInfo{
		Avg:               dt.Avg,
		N:                 dt.N,
		MaxDistance:       dt.MaxDistance,
		Hist:              dt.Hist,
		Timestamp:         dt.Timestamp,
		Version:           dt.Config.Version,
		Name:              dt.Config.Name,
		TargetN:           dt.Config.TargetN,
		TargetUtilization: dt.Config.TargetUtilization,
		NoTarget:          dt.Config.NoTarget,
	}
}

// AddSource adds a source
func (dt *Data) AddSource(dataSource DataSource) error {
	if dataSource == nil {
		return errors.New("DataSource is nil")
	}
	if dt.Sources == nil {
		dt.InitData()
	}
	return dt.Sources.Add(dataSource.GetID(), dataSource, cache.DefaultExpiration)
}

func (dt *Data) GetID() string {
	return dt.Config.Name
}

func (dt *Data) RunOnRandomSources(sourceFunction func(dataSource DataSource) error) error {
	sourceList := dt.Sources.Items()
	sourceLimit := 5                        // This should be configurable
	for _, sourceItem := range sourceList { // Assumption is that random map runs are random enough
		if sourceLimit < 0 {
			break
		}
		sourceLimit--
		source := sourceItem.Object.(DataSource)
		err := sourceFunction(source)
		if err != nil {
			return err
		}
	}
	return nil
}
