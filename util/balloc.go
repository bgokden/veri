package util

import (
	"io/ioutil"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/golang-collections/collections/stack"
)

type Balloc struct {
	BlockSize          uintptr
	Length             int
	MmapHandle         []byte
	StartPointer       uintptr
	Filename           string
	Index              uintptr
	resuableIndexMutex sync.Mutex
	Reuseables         *stack.Stack
}

func NewBalloc(blockSize int, length int) (*Balloc, error) {
	balloc := &Balloc{
		BlockSize: uintptr(blockSize),
		Length:    length,
	}
	ti := blockSize * balloc.Length
	log.Printf("ti : %v\n", ti)
	mapFile, err := ioutil.TempFile("", "balloc")
	if err != nil {
		return nil, err
	}
	balloc.Filename = mapFile.Name()
	_, err = mapFile.Seek(int64(ti-1), 0)
	if err != nil {
		return nil, err
	}
	_, err = mapFile.Write([]byte(" "))
	if err != nil {
		return nil, err
	}
	mmap, err := syscall.Mmap(int(mapFile.Fd()), 0, int(ti), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	balloc.MmapHandle = mmap
	balloc.StartPointer = uintptr(unsafe.Pointer(&mmap[0]))

	balloc.Reuseables = stack.New()
	balloc.Reuseables.Push(uintptr(0))
	return balloc, nil
}

func (b *Balloc) Close(clean bool) error {
	err := syscall.Munmap(b.MmapHandle)
	if err != nil {
		return err
	}
	if clean {
		os.Remove(b.Filename)
	}
	return nil
}

func (b *Balloc) GetNewIndex() uintptr {
	b.resuableIndexMutex.Lock()
	defer b.resuableIndexMutex.Unlock()
	if reusbaleInterface := b.Reuseables.Pop(); reusbaleInterface != nil {
		if resuable, ok := reusbaleInterface.(uintptr); ok {
			return resuable
		}
	}
	index := atomic.AddUintptr(&b.Index, b.BlockSize)
	return index
}

func (b *Balloc) GetByteSlicePtr() *[]byte {
	index := b.GetNewIndex()
	return (*[]byte)(unsafe.Pointer(b.StartPointer + index))
}

// This is only for testing
func (b *Balloc) GetByteSlicePtrByIndex(index int) *[]byte {
	diff := uintptr(index) * b.BlockSize
	return (*[]byte)(unsafe.Pointer(b.StartPointer + diff))
}

func (b *Balloc) Delete(byteSlicePtr *[]byte) error {
	b.resuableIndexMutex.Lock()
	defer b.resuableIndexMutex.Unlock()
	index := uintptr(unsafe.Pointer(byteSlicePtr)) - b.StartPointer
	b.Reuseables.Push(index)
	return nil
}
