package util

import (
	"bytes"
	"compress/zlib"
	"io"
)

// BytesToString Alternative to hex.EncodeToString(b)
func EncodeToString(b []byte) string {
	// bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	// sh := reflect.StringHeader{bh.Data, bh.Len}
	// return *(*string)(unsafe.Pointer(&sh))
	return string(b) // Gave up on Compression since it is expensive but not effective
}

func Compress(src []byte) []byte {
	var b bytes.Buffer
	w, _ := zlib.NewWriterLevel(&b, zlib.BestCompression)
	w.Write(src)
	w.Close()
	return b.Bytes()
}
func Decompress(src []byte) []byte {
	var out bytes.Buffer
	b := bytes.NewReader(src)
	r, _ := zlib.NewReader(b)
	io.Copy(&out, r)
	r.Close()
	return out.Bytes()
}
