package data

import (
	pb "github.com/bgokden/veri/veriservice"
	badger "github.com/dgraph-io/badger/v2"
)

// Delete delete data to internal kv store
func (dt *Data) Delete(datum *pb.Datum) error {
	keyByte, err := GetKeyAsBytes(datum)
	if err != nil {
		return err
	}
	err = dt.DB.Update(func(txn *badger.Txn) error {
		return txn.Delete(keyByte)
	})
	if err != nil {
		return err
	}
	return nil
}
