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

var GlobalMemoli *Memoli

func init() {
	GlobalMemoli = NewGlobalMemoli()
}

type MemoliArena struct {
	BlockSize          uintptr
	Length             int
	MaxSize            uintptr
	MmapHandle         []byte
	StartPointer       uintptr
	Filename           string
	Index              uintptr
	resuableIndexMutex sync.Mutex
	Reuseables         *stack.Stack
}

func getNewMemoliArena(blockSize uintptr, length int) *MemoliArena {
	ma, err := NewMemoliArena(int(blockSize), length)
	if err != nil {
		log.Printf("Memoli Creation Error: %v", err)
		return nil
	}
	return ma
}

func NewMemoliArena(blockSize int, length int) (*MemoliArena, error) {
	memoliArena := &MemoliArena{
		BlockSize: uintptr(blockSize),
		Length:    length,
		MaxSize:   uintptr(blockSize * (length - 1)),
	}
	ti := blockSize * memoliArena.Length
	mapFile, err := ioutil.TempFile("", "memoliarena")
	if err != nil {
		return nil, err
	}
	memoliArena.Filename = mapFile.Name()
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
	memoliArena.MmapHandle = mmap
	memoliArena.StartPointer = uintptr(unsafe.Pointer(&mmap[0]))

	memoliArena.Reuseables = stack.New()
	memoliArena.Reuseables.Push(uintptr(0))
	return memoliArena, nil
}

func (ma *MemoliArena) Close(clean bool) error {
	err := syscall.Munmap(ma.MmapHandle)
	if err != nil {
		return err
	}
	if clean {
		os.Remove(ma.Filename)
	}
	return nil
}

func (ma *MemoliArena) GetNewIndex() uintptr {
	ma.resuableIndexMutex.Lock()
	defer ma.resuableIndexMutex.Unlock()
	if reusbaleInterface := ma.Reuseables.Pop(); reusbaleInterface != nil {
		if resuable, ok := reusbaleInterface.(uintptr); ok {
			return resuable
		}
	}
	index := atomic.AddUintptr(&ma.Index, ma.BlockSize)
	return index
}

func (ma *MemoliArena) GetByteSlicePtr() *[]byte {
	index := ma.GetNewIndex()
	return (*[]byte)(unsafe.Pointer(ma.StartPointer + index))
}

// This is only for testing
func (ma *MemoliArena) GetByteSlicePtrByIndex(index int) *[]byte {
	diff := uintptr(index) * ma.BlockSize
	return (*[]byte)(unsafe.Pointer(ma.StartPointer + diff))
}

func (ma *MemoliArena) Delete(byteSlicePtr *[]byte) error {
	ma.resuableIndexMutex.Lock()
	defer ma.resuableIndexMutex.Unlock()
	index := uintptr(unsafe.Pointer(byteSlicePtr)) - ma.StartPointer
	ma.Reuseables.Push(index)
	return nil
}

func (ma *MemoliArena) New() unsafe.Pointer {
	index := ma.GetNewIndex()
	if index > ma.MaxSize {
		return nil // run out of space
	}
	return unsafe.Pointer(ma.StartPointer + index)
}

func (ma *MemoliArena) Free(ptr unsafe.Pointer) {
	ma.resuableIndexMutex.Lock()
	defer ma.resuableIndexMutex.Unlock()
	index := uintptr(ptr) - ma.StartPointer
	ma.Reuseables.Push(index)
}

type Memoli struct {
	ArenaMap   sync.Map
	Length     int
	BucketSize uintptr
}

func NewGlobalMemoli() *Memoli {
	return &Memoli{
		Length:     1e+6,
		BucketSize: uintptr(256),
	}
}

func (m *Memoli) ArenaKey(size uintptr) uintptr {
	return size % m.BucketSize
}

func (m *Memoli) New(size uintptr) unsafe.Pointer {
	maInterface, _ := m.ArenaMap.LoadOrStore(m.ArenaKey(size), getNewMemoliArena(size, m.Length))
	if maInterface == nil {
		return nil
	}
	if ma, ok := maInterface.(*MemoliArena); ok {
		return ma.New()
	}
	return nil
}

func (m *Memoli) Free(ptr unsafe.Pointer, size uintptr) {
	// size := unsafe.Sizeof(ptr)
	// log.Printf("Size: %v\n", size)
	if maInterface, ok := m.ArenaMap.Load(m.ArenaKey(size)); ok {
		if ma, ok2 := maInterface.(*MemoliArena); ok2 {
			ma.Free(ptr)
		}
	}
}

func (m *Memoli) Close() error {
	m.ArenaMap.Range(func(_, value interface{}) bool {
		if ma, ok := value.(*MemoliArena); ok {
			err := ma.Close(true)
			if err != nil {
				log.Printf("Error %v\n", err) // There is not much to do
			}
		}
		return true
	})
	return nil // For future
}
