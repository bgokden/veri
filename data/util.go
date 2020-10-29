package data

import (
	"math"
	"time"
)

func getCurrentTime() uint64 {
	return uint64(time.Now().Unix())
}

// CalculateAverage calculates average of two arrays divided by n
func CalculateAverage(avg []float64, p []float64, n float64) []float64 {
	if n == 0 {
		return p
	}
	if len(avg) < len(p) {
		avg = make([]float64, len(p))
	}
	for i := 0; i < len(p); i++ {
		avg[i] += p[i] / n
	}
	return avg
}

// VectorDistance calculates distance of two vector by euclidean distance
func VectorDistance(arr1 []float64, arr2 []float64) float64 {
	minLen := min(len(arr1), len(arr2))
	d := euclideanDistance(arr1[:minLen], arr2[:minLen])
	return d
}

// VectorMultiplication calculates elementwise of multiplication of two vectors
func VectorMultiplication(arr1 []float64, arr2 []float64) float64 {
	minLen := min(len(arr1), len(arr2))
	var ret float64
	for i := 0; i < minLen; i++ {
		ret += arr1[i] * arr2[i]
	}
	return ret
}

func euclideanDistance(arr1 []float64, arr2 []float64) float64 {
	var ret float64
	for i := 0; i < len(arr1); i++ {
		tmp := arr1[i] - arr2[i]
		ret += tmp * tmp
	}
	return math.Sqrt(ret) // Sqrt is totally unnecessary for comparisons
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func sum(arr []float64) float64 {
	sum := 0.0
	for _, e := range arr {
		sum += e
	}
	return sum
}
