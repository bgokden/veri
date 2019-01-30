package data_test

import (
	"fmt"
	"testing"
	"time"

	data "github.com/bgokden/veri/data"
)

/*
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
*/

func TestDataBasic(t *testing.T) {
	dt := data.NewData()
	dt.InsertBasic("1", 0.1, 0.2, 0.3) //
	dt.InsertBasic("2", 0.0, 0.0, 0.0)
	dt.InsertBasic("3", 0.0, 0.0, 1.0) //
	dt.InsertBasic("4", 0.0, 1.0, 0.0) //
	dt.InsertBasic("5", 1.0, 0.0, 0.0) //
	dt.InsertBasic("6", 0.0, 0.0, 0.0) //
	dt.InsertBasic("7", 0.0, 0.0, 0.1) //
	dt.InsertBasic("8", 1.0, 1.0, 1.0) //
	time.Sleep(time.Duration(5000) * time.Millisecond)
	dt.GetKnnBasic(3, 0.0, 0.0, 1.0)
	t.Logf("Run")
}

func TestCalculateAverage(t *testing.T) {
	avg := make([]float64, 0)
	p1 := []float64{1.0, 1.0, 1.0}
	avg = data.CalculateAverage(avg, p1, 2)
	t.Logf("Avg %v\n", avg)
	fmt.Printf("Avg %v\n", avg)
}
