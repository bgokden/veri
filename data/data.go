package data

import (
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/bgokden/go-cache"
	badger "github.com/dgraph-io/badger/v3"

	pb "github.com/bgokden/veri/veriservice"
)

type DataSource interface {
	StreamSearch(datumList *pb.Datum, scoredDatumStream chan<- *pb.ScoredDatum, queryWaitGroup *sync.WaitGroup, config *pb.SearchConfig) error
	Insert(datum *pb.Datum, config *pb.InsertConfig) error
	GetDataInfo() *pb.DataInfo
	GetID() string
}

// Data represents a dataset with similar struture
type Data struct {
	Config      *pb.DataConfig
	Path        string
	Avg         []float64
	N           uint64
	MaxDistance float64
	Hist        []float64
	Timestamp   uint64
	DB          *badger.DB
	DBPath      string
	Dirty       bool
	Sources     *cache.Cache
	QueryCache  *cache.Cache
	Initialized bool
}

func (d *Data) GetConfig() *pb.DataConfig {
	return d.Config
}

// NewData creates a data struct
func NewData(config *pb.DataConfig, dataPath string) (*Data, error) {
	dt := &Data{
		Config: config,
	}
	log.Printf("Create Data\n")
	dt.DBPath = path.Join(dataPath, config.Name)
	dt.InitData()
	go dt.Run()
	return dt, nil
}

// NewPreData creates a data struct
func NewPreData(config *pb.DataConfig, dataPath string) *Data {
	dt := &Data{
		Config: config,
	}
	log.Printf("Pre Create Data %v\n", dt.Config)
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
		if nextTime <= getCurrentTime() {
			secondsToSleep := uint64(10) // increment this based on load
			dt.Process(false)
			nextTime = getCurrentTime() + secondsToSleep
			dt.DB.RunValueLogGC(0.7)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

	}
	// return nil
}

// Process runs through keys and calculates statistics
func (dt *Data) Process(force bool) error {
	if dt.Dirty || getCurrentTime()-dt.Timestamp >= 10000 || force {
		log.Printf("Running Process (forced: %v)\n", force)
		n := uint64(0)
		distance := 0.0
		maxDistance := 0.0
		avg := make([]float64, 0)
		hist := make([]float64, 64)
		nFloat := float64(dt.N)
		if nFloat == 0 {
			log.Printf("Data size was 0\n")
			nFloat = 1
		}
		histUnit := 1 / nFloat

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
						hist[index] += histUnit
					}
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
	}
	dt.Timestamp = getCurrentTime() // update always
	dt.Dirty = false
	return nil
}

// GetDataInfo out of data
func (dt *Data) GetDataInfo() *pb.DataInfo {
	log.Printf("Data: %v\n", dt)
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
func (dt *Data) AddSource(dataSource DataSource) {
	if dt.Sources == nil {
		dt.InitData()
	}
	dt.Sources.Set(dataSource.GetID(), dataSource, cache.DefaultExpiration)
}

func (dt *Data) GetID() string {
	return dt.Config.Name
}
