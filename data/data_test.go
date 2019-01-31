package data_test

import (
	"testing"

	data "github.com/bgokden/veri/data"
	"github.com/stretchr/testify/assert"
)

func TestData(t *testing.T) {
	dt := data.NewData()
	key := data.EuclideanPointKey{
		SequenceLengthOne: 1,
		SequenceLengthTwo: 0,
		SequenceDimOne:    10,
		SequenceDimTwo:    0,
	}
	value := data.EuclideanPointValue{
		Timestamp:  0,
		Label:      "a",
		GroupLabel: "a",
	}
	dt.Insert(key, value)
}

func TestDataBasic(t *testing.T) {
	dt := data.NewData()
	dt.InsertBasic("1", 0.1, 0.2, 0.3)
	dt.InsertBasic("2", 0.0, 0.0, 0.0)
	dt.InsertBasic("3", 0.0, 0.0, 1.0)
	dt.InsertBasic("4", 0.0, 1.0, 0.0)
	dt.InsertBasic("5", 1.0, 0.0, 0.0)
	dt.InsertBasic("6", 0.0, 0.0, 0.0)
	dt.InsertBasic("7", 0.0, 0.0, 0.1)
	dt.InsertBasic("8", 1.0, 1.0, 1.0)
	dt.Process(true)
	result, err := dt.GetKnnBasic(3, 0.0, 0.0, 1.0)

	assert.Nil(t, err)
	assert.Equal(t, len(result), 3)

	assert.Equal(t, "3", result[0].GetLabel())
	assert.Equal(t, "1", result[1].GetLabel())
	assert.Equal(t, "7", result[2].GetLabel())
}

func TestCalculateAverage(t *testing.T) {
	avg := make([]float64, 0)
	p1 := []float64{1.0, 1.0, 1.0}
	avg = data.CalculateAverage(avg, p1, 2)
	t.Logf("Avg %v\n", avg)
}
