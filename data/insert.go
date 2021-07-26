package data

import (
	"errors"
	"log"

	pb "github.com/bgokden/veri/veriservice"
)

// Insert inserts data to internal kv store
func (dt *Data) Insert(datum *pb.Datum, config *pb.InsertConfig) error {
	if dt.Config != nil && !dt.Config.NoTarget && dt.N >= dt.Config.TargetN {
		return errors.New("Number of elements is over the target")
	}
	if dt.Initialized == false {
		dt.InitData()
	}
	err := dt.InsertBDMap(datum, config)
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
		dt.RunOnRandomSources(5, func(source DataSource) error {
			err := source.Insert(datum, config)
			if err != nil && CheckIfUnkownError(err) { // This error occurs frequently and it is normal
				log.Printf("Sending Insert error %v\n", err.Error())
			}
			if err == nil {
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
