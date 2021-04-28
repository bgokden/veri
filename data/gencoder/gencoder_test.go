package gencoder_test

import (
	"testing"

	"github.com/bgokden/veri/data/gencoder"
	"github.com/stretchr/testify/assert"
)

func TestDatumScoreEncoding(t *testing.T) {
	ds := gencoder.DatumScore{Score: 3}
	b, e := ds.Marshal()
	assert.Nil(t, e)
	ds2 := gencoder.DatumScore{Score: 0}
	_, e2 := ds2.Unmarshal(b)
	assert.Nil(t, e2)
	assert.Equal(t, ds2.Score, float32(3))
}
