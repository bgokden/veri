package data_test

import (
	"testing"

	data "github.com/bgokden/veri-data"
	"github.com/stretchr/testify/assert"
)

func TestDatum(t *testing.T) {
	datum := data.NewDatum([]float64{0.1, 0.2, 0.3}, 3, 0, 1, 0, []byte("a"), []byte("a"), 0)

	keyByte, err := datum.GetKey()
	assert.Nil(t, err)
	valueByte, err := datum.GetValue()
	assert.Nil(t, err)

	key2, err := data.ToDatumKey(keyByte)
	assert.Nil(t, err)
	assert.Equal(t, datum.Key, key2)
	value2, err := data.ToDatumValue(valueByte)
	assert.Nil(t, err)
	assert.Equal(t, datum.Value, value2)

	datum2, err := data.ToDatum(keyByte, valueByte)
	assert.Nil(t, err)
	assert.Equal(t, datum.Key, datum2.Key)
	assert.Equal(t, datum.Value, datum2.Value)
}
