package data

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/bgokden/veri/veriservice"
	"github.com/dgraph-io/badger/v3"
	pbp "github.com/dgraph-io/badger/v3/pb"
	"github.com/dgraph-io/ristretto/z"
)

func (dt *Data) SyncAll() error {
	var waitGroup sync.WaitGroup
	sourceList := dt.Sources.Items()
	for _, sourceItem := range sourceList {
		source := sourceItem.Object.(DataSource)
		waitGroup.Add(1)
		dt.Sync(source, &waitGroup)
	}
	waitGroup.Wait()
	return nil
}

func isEvictionOn(localInfo *pb.DataInfo, config *pb.DataConfig, deleted uint64) bool {
	lowerThreshold := uint64(float64(localInfo.TargetN) * config.TargetUtilization) // This should be configurable
	if !config.NoTarget && (localInfo.N-deleted) >= lowerThreshold {
		return true
	}
	return false
}

func (dt *Data) Sync(source DataSource, waitGroup *sync.WaitGroup) error {
	defer waitGroup.Done()
	info := source.GetDataInfo()
	if info == nil {
		log.Println("Data info can not be get")
		return errors.New("Data info can not be get")
	}
	localInfo := dt.GetDataInfo()
	localN := localInfo.N
	if localN == 0 {
		return nil // nothing to do
	}
	config := dt.GetConfig()
	diff := minUint64(((localN-info.N)/2)+1, 100)
	if diff > 0 {
		datumStream := make(chan *pb.InsertDatumWithConfig, 100)
		go func() {
			deleted := uint64(0)
			for datum := range datumStream {
				// log.Printf("Sync Insert\n")
				err := source.Insert(datum.Datum, datum.Config)
				if err != nil {
					log.Printf("Sync Insertion Error: %v\n", err.Error())
					break
				}
				if err == nil && isEvictionOn(localInfo, config, deleted) {
					dt.Delete(datum.Datum)
					deleted++
				}
				time.Sleep(200 * time.Millisecond)
			}
		}()
		dt.InsertStreamSample(datumStream, float64(diff)/float64(localN))
		close(datumStream)
	}
	return nil
}

// StreamCollector collects results
type StreamCollector struct {
	DatumStream chan<- *pb.Datum
}

func (dt *Data) StreamAll(datumStream chan<- *pb.Datum) error {
	return dt.StreamSample(datumStream, 1)
}

func (dt *Data) StreamSample(datumStream chan<- *pb.Datum, fraction float64) error {
	c := &StreamCollector{
		DatumStream: datumStream,
	}
	stream := dt.DB.NewStream()
	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = 16                     // Set number of goroutines to use for iteration.
	stream.Prefix = nil                   // Leave nil for iteration over the whole DB.
	stream.LogPrefix = "Badger.Streaming" // For identifying stream logs. Outputs to Logger.

	// ChooseKey is called concurrently for every key. If left nil, assumes true by default.
	if fraction < 1 {
		stream.ChooseKey = func(item *badger.Item) bool {
			return rand.Float64() < fraction
		}
	} else {
		stream.ChooseKey = nil
	}

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
	return nil
}

// Send collects the results
func (c *StreamCollector) Send(buf *z.Buffer) error {
	err := buf.SliceIterate(func(s []byte) error {
		kv := new(pbp.KV)
		if err := kv.Unmarshal(s); err != nil {
			return err
		}

		if kv.StreamDone == true {
			return nil
		}

		datum, errInner := ToDatum(kv.Key, kv.Value)
		if errInner != nil {
			return errInner
		}
		c.DatumStream <- datum
		return nil
	})
	return err
}

// InsertStreamCollector collects results
type InsertStreamCollector struct {
	DatumStream chan<- *pb.InsertDatumWithConfig
}

func (dt *Data) InsertStreamSample(datumStream chan<- *pb.InsertDatumWithConfig, fraction float64) error {
	c := &InsertStreamCollector{
		DatumStream: datumStream,
	}
	stream := dt.DB.NewStream()
	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = 16                     // Set number of goroutines to use for iteration.
	stream.Prefix = nil                   // Leave nil for iteration over the whole DB.
	stream.LogPrefix = "Badger.Streaming" // For identifying stream logs. Outputs to Logger.

	// ChooseKey is called concurrently for every key. If left nil, assumes true by default.
	if fraction < 1 {
		stream.ChooseKey = func(item *badger.Item) bool {
			return rand.Float64() < fraction
		}
	} else {
		stream.ChooseKey = nil
	}

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
	return nil
}

// Send collects the results
func (c *InsertStreamCollector) Send(buf *z.Buffer) error {
	err := buf.SliceIterate(func(s []byte) error {
		kv := new(pbp.KV)
		if err := kv.Unmarshal(s); err != nil {
			return err
		}

		if kv.StreamDone == true {
			return nil
		}

		config := InsertConfigFromExpireAt(kv.ExpiresAt)
		if config.TTL < 10 {
			return nil
		}
		datum, errInner := ToDatum(kv.Key, kv.Value)
		if errInner != nil {
			return errInner
		}
		c.DatumStream <- &pb.InsertDatumWithConfig{
			Datum:  datum,
			Config: config,
		}
		return nil
	})
	return err
}

func InsertConfigFromExpireAt(expiresAt uint64) *pb.InsertConfig {
	timeLeftInSeconds := expiresAt - uint64(getCurrentTime())
	return &pb.InsertConfig{
		TTL:   timeLeftInSeconds,
		Count: 1,
	}
}
