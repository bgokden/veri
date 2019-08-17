package data

import (
	"errors"
	kdtree "github.com/bgokden/go-kdtree"
	gobiteme "github.com/bgokden/go-bite-me"
)

type TempData struct {
	Tree   *kdtree.KDTree
	Map    *gobiteme.GoBiteMe
}

func NewTempData() *TempData {
	dt := TempData{}
	dt.Map = gobiteme.New()
	return &dt
}

// Insert entry into the data
func (dt *TempData) Insert(key *EuclideanPointKey, value *EuclideanPointValue) error {
	keyByte := EncodeEuclideanPointKey(key)
	valueByte := EncodeEuclideanPointValue(value)
	dt.Map.Bite(keyByte, valueByte)
	return nil
}

func (dt *TempData) Process() error {
	n := int64(0)
	points := make([]kdtree.Point, 0)
	dt.Map.ThrowUp(func(e *gobiteme.Bite) {
		euclideanPointKey := DecodeEuclideanPointKey(e.Key)
		euclideanPointValue := DecodeEuclideanPointValue(e.Value)
		point := NewEuclideanPointFromKeyValue(euclideanPointKey, euclideanPointValue)
		points = append(points, point)
		n++
	})
	if len(points) > 0 {
		tree := kdtree.NewKDTree(points)
		dt.Tree = tree
	}
	return nil
}

func (dt *TempData) GetKnn(queryK int64, point *EuclideanPoint) ([]*EuclideanPoint, error) {
	if dt.Tree != nil {
		ans := dt.Tree.KNN(point, int(queryK))
		ret := make([]*EuclideanPoint, len(ans))
		for i := 0; i < len(ans); i++ {
			ret[i] = NewEuclideanPointFromPoint(ans[i])
		}
		return ret, nil
	}
	return []*EuclideanPoint{}, errors.New("Points not initialized yet")
}
