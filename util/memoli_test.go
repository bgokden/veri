package util_test

import (
	"fmt"
	"log"
	"testing"
	"unsafe"

	"github.com/bgokden/veri/util"
	"github.com/stretchr/testify/assert"
)

// This is a test for MemoliArena development
func TestMemoliArena(t *testing.T) {
	n := int(1000)
	size := int(8192)

	b, err := util.NewMemoliArena(size, n)
	assert.Nil(t, err)

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
	assert.Nil(t, err)
}

func TestGlobalMemoli(t *testing.T) {
	arrayBackedByMmap := []*string{}
	log.Println("Start")
	testN := 10
	for i := 0; i < testN; i++ {
		a := fmt.Sprintf("hello world %v", i)
		slicePtr := (*string)(util.GlobalMemoli.New(unsafe.Sizeof(a)))
		if slicePtr != nil {
			log.Printf("slicePtr: %X\n", unsafe.Pointer(slicePtr))
			*slicePtr = a
		}
		arrayBackedByMmap = append(arrayBackedByMmap, slicePtr)
	}

	for i := 0; i < 3; i++ {
		slicePtr := arrayBackedByMmap[i]
		util.GlobalMemoli.Free(unsafe.Pointer(slicePtr))
	}

	for i := 20; i < 20+testN; i++ {
		a := fmt.Sprintf("hello world %v", i)
		slicePtr := (*string)(util.GlobalMemoli.New(unsafe.Sizeof(a)))
		if slicePtr != nil {
			log.Printf("slicePtr: %X\n", unsafe.Pointer(slicePtr))
			*slicePtr = a
		}
		arrayBackedByMmap = append(arrayBackedByMmap, slicePtr)
	}

	for i, slicePtr := range arrayBackedByMmap {
		fmt.Printf("%v -> %p = _%v_\n", i, slicePtr, *slicePtr)
	}
}
