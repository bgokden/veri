package data_test

import (
	"testing"

	"math/rand"

	data "github.com/bgokden/veri/data"
)

func randFloats64(min, max float64, n int) []float64 {
	res := make([]float64, n)
	for i := range res {
		res[i] = min + rand.Float64()*(max-min)
	}
	return res
}

func randFloats32(min, max float32, n int) []float32 {
	res := make([]float32, n)
	for i := range res {
		res[i] = min + rand.Float32()*(max-min)
	}
	return res
}

var vector64_0 = randFloats64(-1, 1, 512)
var vector64_1 = randFloats64(-1, 1, 512)

var vector32_0 = randFloats32(-1, 1, 512)
var vector32_1 = randFloats32(-1, 1, 512)

func BenchmarkCosineSimilarity64(b *testing.B) {

	for i := 0; i < b.N; i++ {
		data.CosineSimilarity(vector64_0, vector64_1)
	}
}

func BenchmarkCosineSimilarity32(b *testing.B) {

	for i := 0; i < b.N; i++ {
		data.CosineSimilarity32(vector32_0, vector32_1)
	}
}
