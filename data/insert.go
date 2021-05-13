package data

import (
	"errors"
	"log"
	"time"

	pb "github.com/bgokden/veri/veriservice"
	badger "github.com/dgraph-io/badger/v3"
)

// Insert inserts data to internal kv store
func (dt *Data) Insert(datum *pb.Datum, config *pb.InsertConfig) error {
	if dt.Config != nil && !dt.Config.NoTarget && dt.N >= dt.Config.TargetN {
		return errors.New("Number of elements is over the target")
	}
	var ttlDuration *time.Duration
	if config != nil && config.GetTTL() > 0 {
		// log.Printf("Insert Datum with ttl config: %v\n", config.GetTTL())
		d := time.Duration(config.GetTTL()) * time.Second
		ttlDuration = &d
	}
	keyByte, err := GetKeyAsBytes(datum)
	if err != nil {
		return err
	}
	valueByte, err := GetValueAsBytes(datum)
	if err != nil {
		return err
	}
	err = dt.DB.Update(func(txn *badger.Txn) error {
		if ttlDuration != nil {
			// log.Printf("Insert Datum with ttl: %v\n", ttlDuration)
			e := badger.NewEntry(keyByte, valueByte).WithTTL(*ttlDuration)
			return txn.SetEntry(e)
		}
		// log.Printf("Insert Datum: %v ttl: %v\n", datum, ttlDuration)
		return txn.Set(keyByte, valueByte)
	})
	if err != nil {
		return err
	}
	dt.Dirty = true
	if config == nil {
		config = &pb.InsertConfig{
			TTL:   0,
			Count: 0,
		}
	}
	counter := uint32(1)
	if dt.Config.EnforceReplicationOnInsert && config.Count == 0 {
		config.Count++
		// log.Printf("Sending Insert with config.Count: %v ttl: %v\n", config.Count, config.TTL)
		dt.RunOnRandomSources(func(source DataSource) error {
			err := source.Insert(datum, config)
			if err != nil {
				log.Printf("Sending Insert error %v\n", err.Error())
			} else {
				counter++
			}
			if counter >= dt.Config.ReplicationOnInsert {
				return errors.New("Replication number reached")
			}
			return nil
		})
		if counter < dt.Config.ReplicationOnInsert {
			return errors.New("Replicas is less then Replication Config")
		}
	}
	return nil
}
