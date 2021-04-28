package data

import (
	"math"
	"time"

	"github.com/chewxy/math32"
)

func getCurrentTime() uint64 {
	return uint64(time.Now().Unix())
}

// CalculateAverage calculates average of two arrays divided by n
func CalculateAverage(avg []float32, p []float32, n float32) []float32 {
	if n == 0 {
		return p
	}
	if len(avg) < len(p) {
		avg = make([]float32, len(p))
	}
	for i := 0; i < len(p); i++ {
		avg[i] += p[i] / n
	}
	return avg
}

func QuickVectorDistance(arr1 []float32, arr2 []float32) float64 {
	minLen := min(len(arr1), len(arr2))
	var ret float64
	for i := 0; i < minLen; i++ {
		tmp := arr1[i] - arr2[i]
		ret += float64(math32.Abs(tmp))
	}
	return ret
}

// VectorDistance calculates distance of two vector by euclidean distance
func VectorDistance(arr1 []float32, arr2 []float32) float64 {
	minLen := min(len(arr1), len(arr2))
	d := euclideanDistance(arr1[:minLen], arr2[:minLen])
	return d
}

// VectorMultiplication calculates elementwise of multiplication of two vectors
func VectorMultiplication(arr1 []float32, arr2 []float32) float64 {
	minLen := min(len(arr1), len(arr2))
	var ret float32
	for i := 0; i < minLen; i++ {
		ret += arr1[i] * arr2[i]
	}
	return float64(ret)
}

// CosineSimilarity for vector similarity
func CosineSimilarity(a []float32, b []float32) float64 {
	count := 0
	lengthA := len(a)
	lengthB := len(b)
	if lengthA > lengthB {
		count = lengthA
	} else {
		count = lengthB
	}
	sumA := float32(0.0)
	s1 := float32(0.0)
	s2 := float32(0.0)
	for k := 0; k < count; k++ {
		if k >= lengthA {
			s2 += math32.Pow(b[k], 2)
			continue
		}
		if k >= lengthB {
			s1 += math32.Pow(a[k], 2)
			continue
		}
		sumA += a[k] * b[k]
		s1 += math32.Pow(a[k], 2)
		s2 += math32.Pow(b[k], 2)
	}
	if s1 == 0 || s2 == 0 {
		return 0.0
	}
	return float64(sumA / (math32.Sqrt(s1) * math32.Sqrt(s2)))
}

// CosineSimilarity64 for vector similarity
func CosineSimilarity64(a []float64, b []float64) float64 {
	count := 0
	lengthA := len(a)
	lengthB := len(b)
	if lengthA > lengthB {
		count = lengthA
	} else {
		count = lengthB
	}
	sumA := 0.0
	s1 := 0.0
	s2 := 0.0
	for k := 0; k < count; k++ {
		if k >= lengthA {
			s2 += math.Pow(b[k], 2)
			continue
		}
		if k >= lengthB {
			s1 += math.Pow(a[k], 2)
			continue
		}
		sumA += a[k] * b[k]
		s1 += math.Pow(a[k], 2)
		s2 += math.Pow(b[k], 2)
	}
	if s1 == 0 || s2 == 0 {
		return 0.0
	}
	return sumA / (math.Sqrt(s1) * math.Sqrt(s2))
}

// CosineSimilarity for vector similarity
func CosineSimilarity32(a []float32, b []float32) float32 {
	count := 0
	lengthA := len(a)
	lengthB := len(b)
	if lengthA > lengthB {
		count = lengthA
	} else {
		count = lengthB
	}
	sumA := float32(0.0)
	s1 := float32(0.0)
	s2 := float32(0.0)
	for k := 0; k < count; k++ {
		if k >= lengthA {
			s2 += b[k] * b[k]
			continue
		}
		if k >= lengthB {
			s1 += a[k] * a[k]
			continue
		}
		sumA += a[k] * b[k]
		s1 += a[k] * a[k]
		s2 += b[k] * b[k]
	}
	if s1 == 0 || s2 == 0 {
		return 0.0
	}
	return sumA / float32(math.Sqrt(float64(s1))*math.Sqrt(float64(s2)))
}

func euclideanDistance(arr1 []float32, arr2 []float32) float64 {
	var ret float32
	for i := 0; i < len(arr1); i++ {
		tmp := arr1[i] - arr2[i]
		ret += math32.Pow(tmp, 2)
	}
	return math.Sqrt(float64(ret)) // Sqrt is totally unnecessary for comparisons
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minUint64(a, b uint64) uint64 {
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
