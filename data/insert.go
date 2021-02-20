package data

import (
	"errors"
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
			// log.Printf("Insert Datum: %v ttl: %v\n", datum, ttlDuration)
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
	return nil
}
