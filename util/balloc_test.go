package util_test

import (
	"fmt"
	"log"
	"os"
	"testing"
	"unsafe"

	"github.com/bgokden/veri/util"
)

// This is a test for balloc development
func TestBalloc(t *testing.T) {
	n := int(1000)
	size := int(8192)

	b, err := util.NewBalloc(size, n)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	arrayBackedByMmap := []*[]byte{}

	log.Println("Start")
	testN := 10
	for i := 0; i < testN; i++ {
		a := []byte(fmt.Sprintf("hello world %v", i))
		slicePtr := b.GetByteSlicePtr()
		if slicePtr != nil {
			log.Printf("slicePtr: %X\n", unsafe.Pointer(slicePtr))
			*slicePtr = a
		}
		arrayBackedByMmap = append(arrayBackedByMmap, slicePtr)
	}

	for i := 0; i < testN; i++ {
		slicePtr := b.GetByteSlicePtrByIndex(i)
		fmt.Printf("_%v_\n", string(*slicePtr))
	}

	// delete first 3
	for i := 0; i < 3; i++ {
		slicePtr := arrayBackedByMmap[i]
		b.Delete(slicePtr)
	}

	// re-write
	for i := 20; i < 20+testN; i++ {
		a := []byte(fmt.Sprintf("hello world %v", i))
		slicePtr := b.GetByteSlicePtr()
		if slicePtr != nil {
			*slicePtr = a
		}
		arrayBackedByMmap = append(arrayBackedByMmap, slicePtr)
	}

	// re-print
	for i, byteSlice := range arrayBackedByMmap {
		fmt.Printf("%v -> %p = _%v_\n", i, byteSlice, string(*byteSlice))
	}

	fmt.Println("done")

	err = b.Close(true)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	// assert.NotNil(t, nil)
}
