package data

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v2"
	bpb "github.com/dgraph-io/badger/v2/pb"
)

func (dt *Data) SyncAll() error {
	var waitGroup sync.WaitGroup
	sourceList := dt.Sources.Items()
	for _, sourceItem := range sourceList {
		source := sourceItem.Object.(DataSource)
		waitGroup.Add(1)
		go dt.Sync(source, &waitGroup)
	}
	waitGroup.Wait()
	return nil
}

func (dt *Data) Sync(source DataSource, waitGroup *sync.WaitGroup) error {
	info := source.GetDataInfo()
	localN := dt.GetDataInfo().N
	diff := (localN - info.N) / 2
	if diff > 0 {
		datumStream := make(chan *Datum, 100)
		go func() {
			for datum := range datumStream {
				source.Insert(datum, nil)
			}
		}()
		dt.StreamSample(datumStream, float64(diff)/float64(localN))
		close(datumStream)
	}
	waitGroup.Done()
	return nil
}

// StreamCollector collects results
type StreamCollector struct {
	DatumStream chan<- *Datum
}

func (dt *Data) StreamAll(datumStream chan<- *Datum) error {
	return dt.StreamSample(datumStream, 1)
}

func (dt *Data) StreamSample(datumStream chan<- *Datum, fraction float64) error {
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
func (c *StreamCollector) Send(list *bpb.KVList) error {
	for _, item := range list.Kv {
		datum, _ := ToDatum(item.Key, item.Value)
		c.DatumStream <- datum

	}
	return nil
}

// InsertStreamCollector collects results
type InsertStreamCollector struct {
	DatumStream chan<- *InsertDatumWithConfig
}

func (dt *Data) InsertStreamSample(datumStream chan<- *InsertDatumWithConfig, fraction float64) error {
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
func (c *InsertStreamCollector) Send(list *bpb.KVList) error {
	for _, item := range list.Kv {
		config := InsertConfigFromExpireAt(item.ExpiresAt)
		if config.TTL < 10*time.Second {
			continue
		}
		datum, _ := ToDatum(item.Key, item.Value)
		c.DatumStream <- &InsertDatumWithConfig{
			Datum:  datum,
			Config: config,
		}

	}
	return nil
}

func InsertConfigFromExpireAt(expiresAt uint64) *InsertConfig {
	timeLeftInSeconds := expiresAt - uint64(getCurrentTime())
	return &InsertConfig{
		TTL: time.Duration(timeLeftInSeconds) * time.Second,
	}
}
