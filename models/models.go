package models

type InternalDatumKey struct {
	Feature    []float32
	GroupLabel []byte
	Size1      uint32
	Size2      uint32
	Dim1       uint32
	Dim2       uint32
}

type InternalDatumValue struct {
	Version uint64
	Label   []byte
}

type InternalDatum struct {
	Key   InternalDatumKey
	Value InternalDatumValue
}
