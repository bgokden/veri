package data

import (
	"errors"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bgokden/go-cache"

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
	DataIndex     *[]*pb.Datum
	AnnoyIndex    annoyindex.AnnoyIndexAngular
	BuildFileName string
}

// Data represents a dataset with similar struture
type Data struct {
	sync.RWMutex
	Config            *pb.DataConfig
	Path              string
	Avg               []float32
	N                 uint64
	MaxDistance       float64
	Hist              []float32
	Timestamp         uint64
	LastRunTimestamp  uint64
	DBPath            string
	Dirty             bool
	Sources           *cache.Cache
	QueryCache        *cache.Cache
	Initialized       bool
	Alive             bool
	Annoyer           Annoyer
	Runs              int32
	DBMap             sync.Map
	RecentInsertCount uint64
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
	atomic.StoreUint64(&(dt.RecentInsertCount), 0)
	// log.Printf("Pre Create Data %v\n", dt.Config)
	dt.DBPath = path.Join(dataPath, config.Name)
	return dt
}

func (dt *Data) InitData() error {
	log.Printf("Init Data %v\n", dt.Config)
	dt.Lock()
	defer dt.Unlock()
	if dt.Initialized == false {
		dt.Sources = cache.New(10*time.Minute, 1*time.Minute)
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
	return dt, nil
}

// Close currently closes underlying kv store
func (dt *Data) Close() error {
	dt.Alive = false
	if dt.Sources != nil && len(dt.Sources.Items()) > 0 {
		for dt.N > 0 {
			dt.Process(true)
		}
	}
	// return dt.DB.Close()
	return nil
}

// Delete currently deletes underlying data folder ignores errors.
func (dt *Data) DeletePath() error {
	// dt.DB.Close()
	os.RemoveAll(dt.DBPath)
	return nil
}

// Run runs statistical calculation regularly
func (dt *Data) Run() error {
	if atomic.LoadInt32(&dt.Runs) >= 1 {
		log.Printf("Multiple Run calls detected.")
		return errors.New("Another instance of processor is running for data")
	}
	atomic.AddInt32(&dt.Runs, 1)
	defer atomic.AddInt32(&dt.Runs, -1)
	nextTime := getCurrentTime()
	gcCounter := 10
	for {
		if !dt.Alive {
			break
		}
		if nextTime <= getCurrentTime() {
			secondsToSleep := uint64(10) // increment this based on load
			dt.Process(false)
			nextTime = getCurrentTime() + secondsToSleep
			gcCounter--
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

	}
	return nil
}

func (dt *Data) DataSourceDiffMap() (map[string]uint64, uint64) {
	localInfo := dt.GetDataInfo()
	localN := localInfo.N
	diffMap := map[string]uint64{}
	sum := uint64(0)
	dt.RunOnRandomSources(5, func(source DataSource) error {
		info := source.GetDataInfo()
		if info != nil {
			diff := minUint64(((localN-info.N)/2)+1, 1000) // diff may be negative
			fraction := float64(diff) / float64(localN+1)
			if info.N > localN {
				diff = 1
			}
			if VectorDistance(localInfo.Avg, info.Avg)+VectorDistance(localInfo.Hist, info.Hist) <= 0.01*localInfo.GetMaxDistance() { // This is arbitary
				diff = 1             // close enough
				if fraction < 0.01 { // small or negative
					diff = 0
				}
			}
			diffMap[source.GetID()] = diff
			sum += diff
		}
		return nil
	})
	return diffMap, sum
}

func CheckIfUnkownError(err error) bool {
	if strings.Contains(err.Error(), "Number of elements is over the target") || strings.Contains(err.Error(), "Node is in drain mode") {
		return false
	}
	return true
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
	if dt.Sources == nil {
		return errors.New("Sources is still nil")
	}
	return dt.Sources.Add(dataSource.GetID(), dataSource, cache.DefaultExpiration)
}

func (dt *Data) GetID() string {
	return dt.Config.Name
}

func (dt *Data) RunOnRandomSources(sourceLimit int, sourceFunction func(dataSource DataSource) error) error {
	sourceList := dt.Sources.Items()
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
