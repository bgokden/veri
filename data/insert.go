package data

import (
	"errors"
	"time"

	pb "github.com/bgokden/veri/veriservice"
	badger "github.com/dgraph-io/badger/v2"
)

// Insert inserts data to internal kv store
func (dt *Data) Insert(datum *pb.Datum, config *pb.InsertConfig) error {
	if dt.N >= dt.TargetN {
		return errors.New("Number of elements is over the target")
	}
	var ttlDuration *time.Duration
	if config != nil {
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
			e := badger.NewEntry(keyByte, valueByte).WithTTL(*ttlDuration)
			return txn.SetEntry(e)
		}
		return txn.Set(keyByte, valueByte)
	})
	if err != nil {
		return err
	}
	return nil
}
