package util

// BytesToString Alternative to hex.EncodeToString(b)
func EncodeToString(b []byte) string {
	// bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	// sh := reflect.StringHeader{bh.Data, bh.Len}
	// return *(*string)(unsafe.Pointer(&sh))
	return string(b)
}
