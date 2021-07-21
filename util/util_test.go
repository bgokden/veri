package util_test

import (
	crand "crypto/rand"
	"math/rand"
	"testing"

	"github.com/bgokden/veri/util"
	"github.com/stretchr/testify/assert"
)

// GenerateRandomBytes returns securely generated random bytes.
// It will return an error if the system's secure random
// number generator fails to function correctly, in which
// case the caller should not continue.
func GenerateRandomBytes() ([]byte, error) {
	n := 10 + rand.Intn(100)
	b := make([]byte, n)
	_, err := crand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return nil, err
	}
	return b, nil
}

func TestEncodeToStringBasic(t *testing.T) {

	/***************************************/
	byteArray1 := []byte{'A', 'B', 'C', 17, 0, 2, 5}
	str1 := util.EncodeToString(byteArray1)

	/****************************************/

	byteArray2 := []byte{'A', 'B', 'C', 17, 0, 3, 5}
	str2 := util.EncodeToString(byteArray2)

	assert.NotEqual(t, str1, str2)
}

// TODO: This case requires more testing. Can a conflict occur if I use just string()
func TestEncodeToStringConflict(t *testing.T) {

	conflictMap := map[string][]byte{}
	for i := 0; i < 10e3; i++ {
		randomBytes, err := GenerateRandomBytes()
		assert.Nil(t, err)
		randomBytesStr := util.EncodeToString(randomBytes)
		if b, ok := conflictMap[randomBytesStr]; ok {
			assert.Equal(t, randomBytesStr, b)
		} else {
			conflictMap[randomBytesStr] = randomBytes
		}
	}
}

func TestStringConflict(t *testing.T) {

	conflictMap := map[string][]byte{}
	for i := 0; i < 10e6; i++ {
		randomBytes, err := GenerateRandomBytes()
		assert.Nil(t, err)
		randomBytesStr := string(randomBytes)
		if b, ok := conflictMap[randomBytesStr]; ok {
			assert.Equal(t, randomBytesStr, b)
		} else {
			conflictMap[randomBytesStr] = randomBytes
		}
	}
}

func TestCompressDecompress(t *testing.T) {
	input := []byte("Hello this is an example input lkhldkfhldshfl jhfjdshkfhsd hkfsdhkfhds jhlhlshdlfsd ")
	compressed := util.Compress(input)
	decompresed := util.Decompress(compressed)
	assert.Less(t, len(compressed), len(input))
	assert.Equal(t, len(compressed), len(input)-7)
	assert.Equal(t, len(decompresed), len(input))
	assert.Equal(t, decompresed, input)
}
