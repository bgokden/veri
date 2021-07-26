package data

import (
	pb "github.com/bgokden/veri/veriservice"
)

// Delete delete data to internal kv store
func (dt *Data) Delete(datum *pb.Datum) error {
	return dt.DeleteBDMap(datum)
}
