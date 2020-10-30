package data

import (
	"bytes"
	"encoding/gob"
	"log"

	pb "github.com/bgokden/veri/veriservice"
)

// NewDatum is an utily function to initialize datum type
func NewDatum(feature []float64,
	dim1 uint32,
	dim2 uint32,
	size1 uint32,
	size2 uint32,
	groupLabel []byte,
	label []byte,
	version uint64,
) *pb.Datum {
	return &pb.Datum{
		Key: &pb.DatumKey{
			Feature:    feature,
			Dim1:       dim1,
			Dim2:       dim2,
			Size1:      size1,
			Size2:      size1,
			GroupLabel: groupLabel,
		},
		Value: &pb.DatumValue{
			Label:   label,
			Version: version,
		},
	}
}

func GetKeyAsBytes(datum *pb.Datum) ([]byte, error) {
	var byteBuffer bytes.Buffer
	encoder := gob.NewEncoder(&byteBuffer)
	if err := encoder.Encode(*datum.Key); err != nil {
		log.Printf("Encoding error %v\n", err)
		return nil, err
	}
	return byteBuffer.Bytes(), nil
}

func GetValueAsBytes(datum *pb.Datum) ([]byte, error) {
	var byteBuffer bytes.Buffer
	encoder := gob.NewEncoder(&byteBuffer)
	if err := encoder.Encode(*datum.Value); err != nil {
		log.Printf("Encoding error %v\n", err)
		return nil, err
	}
	return byteBuffer.Bytes(), nil
}

func ToDatumKey(byteArray []byte) (*pb.DatumKey, error) {
	var element pb.DatumKey
	r := bytes.NewReader(byteArray)
	decoder := gob.NewDecoder(r)
	if err := decoder.Decode(&element); err != nil {
		log.Printf("Decoding error %v\n", err)
		return nil, err
	}
	return &element, nil
}

func ToDatumValue(byteArray []byte) (*pb.DatumValue, error) {
	var element pb.DatumValue
	r := bytes.NewReader(byteArray)
	decoder := gob.NewDecoder(r)
	if err := decoder.Decode(&element); err != nil {
		log.Printf("Decoding error %v\n", err)
		return nil, err
	}
	return &element, nil
}

func ToDatum(key, value []byte) (*pb.Datum, error) {
	keyP, err := ToDatumKey(key)
	if err != nil {
		return nil, err
	}
	valueP, err := ToDatumValue(value)
	if err != nil {
		return nil, err
	}
	return &pb.Datum{
		Key:   keyP,
		Value: valueP,
	}, nil
}
