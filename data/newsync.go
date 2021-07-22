package data

import (
	"errors"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"time"
	"unsafe"

	"github.com/bgokden/veri/annoyindex"
	"github.com/bgokden/veri/util"
	pb "github.com/bgokden/veri/veriservice"
)

type DBMapEntry struct {
	ExprireAt int64
	Datum     *pb.Datum
}

func NewAllocadtedDatum(datum *pb.Datum) *pb.Datum {
	ptrKey := (*pb.DatumKey)(util.GlobalMemoli.New(unsafe.Sizeof(*(datum.Key))))
	if ptrKey == nil {
		return nil
	}
	ptrValue := (*pb.DatumValue)(util.GlobalMemoli.New(unsafe.Sizeof(*(datum.Value))))
	if ptrValue == nil {
		util.GlobalMemoli.Free(unsafe.Pointer(ptrKey))
		return nil
	}
	newDatum := &pb.Datum{
		Key:   ptrKey,
		Value: ptrValue,
	}
	*(newDatum.Key) = *datum.Key
	*(newDatum.Value) = *datum.Value
	return newDatum
}

func FreeAllocadtedDatum(datum *pb.Datum) {
	util.GlobalMemoli.Free(unsafe.Pointer(datum.Key))
	util.GlobalMemoli.Free(unsafe.Pointer(datum.Value))
}

func (dt *Data) InsertBDMap(datum *pb.Datum, config *pb.InsertConfig) error {
	exprireAt := int64(0)
	if config != nil && config.TTL != 0 {
		exprireAt = time.Now().Unix() + int64(config.TTL)
	}
	newDatum := NewAllocadtedDatum(datum)
	if newDatum == nil {
		return errors.New("Running out of reserved memory")
	}
	entry := &DBMapEntry{
		ExprireAt: exprireAt,
		Datum:     newDatum,
	}
	keyByte, err := GetKeyAsBytes(datum)
	if err != nil {
		return err
	}

	dt.DBMap.Store(util.EncodeToString(keyByte), entry)
	return nil
}

func (dt *Data) DeleteBDMap(datum *pb.Datum) error {
	keyByte, err := GetKeyAsBytes(datum)
	if err != nil {
		return err
	}
	dt.DBMap.Delete(util.EncodeToString(keyByte))
	FreeAllocadtedDatum(datum)
	return nil
}

func (dt *Data) LoopDBMap(entryFunction func(entry *DBMapEntry) error) error {
	var lastError error
	dt.DBMap.Range(func(key, value interface{}) bool {
		if mapEntry, ok := value.(*DBMapEntry); ok {
			if mapEntry.ExprireAt != 0 && mapEntry.ExprireAt <= time.Now().Unix() {
				dt.DBMap.Delete(key)
				return true
			}
			err := entryFunction(mapEntry)
			if err != nil {
				lastError = err
				return false
			}
		}
		return true
	})
	return lastError
}

func (dt *Data) Process(force bool) error {
	if getCurrentTime()-dt.Timestamp >= 60 || force {
		localInfo := dt.GetDataInfo()
		localN := localInfo.N
		config := dt.GetConfig()
		diffMap, limit := dt.DataSourceDiffMap()
		datumStream := make(chan *pb.InsertDatumWithConfig, limit)
		defer close(datumStream)
		insertionCounter := uint64(0)
		fraction := float64(0)
		if localN > 0 {
			fraction = float64(limit) / float64(localN)
			countMap := make(map[string]uint64, len(diffMap))
			go func() {
				deleted := uint64(0)
				counter := 0
				for datum := range datumStream {
					for id, count := range diffMap {
						if countMap[id] < count {
							if sourceItem, ok := dt.Sources.Get(id); ok {
								if source, ok2 := sourceItem.(DataSource); ok2 {
									err := source.Insert(datum.Datum, datum.Config)
									if err != nil && CheckIfUnkownError(err) {
										log.Printf("Sending Insert error %v\n", err.Error())
									}
									if err == nil {
										counter++
									}
									if err == nil && (!dt.Alive || isEvictionOn(localInfo, config, deleted)) {
										countMap[id]++
										dt.DeleteBDMap(datum.Datum)
										deleted++
									}
								}
							}
						}
					}
				}
			}()
		}
		n := uint64(0)
		distance := 0.0
		maxDistance := 0.0
		avg := make([]float32, 0)
		hist := make([]float32, 64)
		nFloat := float32(dt.N)
		if nFloat == 0 {
			nFloat = 1
		}
		histUnit := 1 / nFloat
		newDataIndex := make([]*pb.Datum, max(1000, int(dt.N)))
		var newAnnoyIndex annoyindex.AnnoyIndexAngular
		var newTempFileName string

		err := dt.LoopDBMap(func(entry *DBMapEntry) error {
			n++
			avg = CalculateAverage(avg, entry.Datum.Key.Feature, nFloat)
			distance = VectorDistance(dt.Avg, entry.Datum.Key.Feature)
			if distance > maxDistance {
				maxDistance = distance
			}
			if dt.MaxDistance != 0 {
				index := int((distance / dt.MaxDistance) * 64)
				if index >= 64 {
					index = 63
				}
				if index <= 0 {
					index = 0
				}
				hist[index] += histUnit
			}
			i := int(n - 1)
			if dt.Alive && i < len(newDataIndex) {
				if newAnnoyIndex == nil {
					// newAnnoyIndex = annoyindex.NewAnnoyIndexEuclidean(len(datum.Key.Feature))
					newAnnoyIndex = annoyindex.NewAnnoyIndexAngular(len(entry.Datum.Key.Feature))
					tmpfile, err := ioutil.TempFile("", "annoy")
					if err == nil {
						newTempFileName = tmpfile.Name()
						newAnnoyIndex.OnDiskBuild(newTempFileName)
					}

				}
				feature := make([]float32, len(entry.Datum.Key.Feature))
				copy(feature, entry.Datum.Key.Feature)
				for i, e := range entry.Datum.Key.Feature {
					feature[i] = e
				}
				newAnnoyIndex.AddItem(i, feature)
				newDataIndex[i] = entry.Datum
			}
			if !dt.Alive || (insertionCounter < limit && rand.Float64() < fraction) {
				config := InsertConfigFromExpireAt(uint64(entry.ExprireAt))
				if config.TTL > 10 {
					datumStream <- &pb.InsertDatumWithConfig{
						Datum:  entry.Datum,
						Config: config,
					}
					insertionCounter++
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
			start := time.Now()
			newAnnoyIndex.Build(-1) // Previosly 10, -1 creates index dynamically
			elapsed := time.Since(start)
			log.Printf("Building annoy index took %s", elapsed)
			// log.Printf("Updating index. len: %v\n", len(newDataIndex))
			dt.Annoyer.Lock()
			if dt.Annoyer.DataIndex != nil {
				dt.Annoyer.AnnoyIndex.Unload() // Not sure if this is needed
				annoyindex.DeleteAnnoyIndexAngular(dt.Annoyer.AnnoyIndex)
			}
			oldFile := dt.Annoyer.BuildFileName
			dt.Annoyer.BuildFileName = newTempFileName
			dt.Annoyer.AnnoyIndex = newAnnoyIndex
			dt.Annoyer.DataIndex = &newDataIndex
			dt.Annoyer.Unlock()
			if len(oldFile) > 0 {
				os.Remove(oldFile)
			}
		}
	}
	dt.Dirty = false
	return nil
}
