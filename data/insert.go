package data

import (
	"errors"
	"time"

	badger "github.com/dgraph-io/badger/v2"
)

type InsertConfig struct {
	TTL time.Duration
}

type InsertDatumWithConfig struct {
	Config *InsertConfig
	Datum  *Datum
}

// Insert inserts data to internal kv store
func (dt *Data) Insert(datum *Datum, config *InsertConfig) error {
	if dt.N >= dt.TargetN {
		return errors.New("Number of elements is over the target")
	}
	var ttlDuration *time.Duration
	if config != nil {
		ttlDuration = &config.TTL
	}
	keyByte, err := datum.GetKey()
	if err != nil {
		return err
	}
	valueByte, err := datum.GetValue()
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

// StreamInsert inserts data in stream
func (dt *Data) StreamInsert(datumStream <-chan *InsertDatumWithConfig) error {
	for e := range datumStream {
		dt.Insert(e.Datum, e.Config)
	}
	return nil
}
