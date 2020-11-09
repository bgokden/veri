package data

import (
	"sort"
	"time"

	pb "github.com/bgokden/veri/veriservice"
	"github.com/patrickmn/go-cache"
)

type AggregatorInterface interface {
	Insert(*pb.ScoredDatum) error
	One() *pb.ScoredDatum
	Result() []*pb.ScoredDatum
}

type Aggregator struct {
	Config           *pb.SearchConfig
	List             []*pb.ScoredDatum
	DeDuplicationMap *cache.Cache
	Grouped          bool
}

func NewAggrator(config *pb.SearchConfig, grouped bool) AggregatorInterface {
	a := &Aggregator{
		Config:  config,
		Grouped: grouped,
	}
	a.DeDuplicationMap = cache.New(5*time.Minute, 10*time.Minute)
	return a
}

func (a *Aggregator) IsNewScoredBetter(old, new float64) bool {
	if a.Config.HigherIsBetter {
		if new > old {
			return true
		}
	} else {
		if old < new {
			return true
		}
	}
	return false
}

func (a *Aggregator) InsertToList(scoredDatum *pb.ScoredDatum) error {
	itemAdded := false
	limit := a.Config.Limit
	if uint32(len(a.List)) < limit {
		a.List = append(a.List, scoredDatum)
		itemAdded = true
	} else if a.IsNewScoredBetter(a.List[len(a.List)-1].Score, scoredDatum.Score) {
		a.List[len(a.List)-1] = scoredDatum
		itemAdded = true
	}
	if itemAdded {
		if a.Config.HigherIsBetter {
			sort.Slice(a.List, func(i, j int) bool {
				return a.List[i].Score > a.List[j].Score
			})
		} else {
			sort.Slice(a.List, func(i, j int) bool {
				return a.List[i].Score < a.List[j].Score
			})
		}
		itemAdded = false
	}
	return nil
}

func (a *Aggregator) Insert(scoredDatum *pb.ScoredDatum) error {
	if a.Grouped {
		keyString := string(scoredDatum.Datum.Key.GroupLabel)
		if aGroupAggregatorInterface, ok := a.DeDuplicationMap.Get(keyString); ok {
			aGroupAggregator := aGroupAggregatorInterface.(AggregatorInterface)
			return aGroupAggregator.Insert(scoredDatum)
		} else {
			aGroupAggregator := NewAggrator(a.Config, false)
			a.DeDuplicationMap.Set(keyString, aGroupAggregator, cache.NoExpiration)
			return aGroupAggregator.Insert(scoredDatum)
		}
	} else {
		keyByte, err := GetKeyAsBytes(scoredDatum.GetDatum())
		if err != nil {
			return err
		}
		keyString := string(keyByte)
		if previousScore, ok := a.DeDuplicationMap.Get(keyString); ok {
			if a.IsNewScoredBetter(previousScore.(float64), scoredDatum.GetScore()) {
				a.DeDuplicationMap.Set(keyString, scoredDatum.GetScore(), cache.NoExpiration)
				return a.InsertToList(scoredDatum)
			}
		} else {
			a.DeDuplicationMap.Set(keyString, scoredDatum.GetScore(), cache.NoExpiration)
			return a.InsertToList(scoredDatum)
		}

	}
	return nil
}

func (a *Aggregator) Result() []*pb.ScoredDatum {
	if a.Grouped {
		groupMap := a.DeDuplicationMap.Items()
		agg := NewAggrator(a.Config, false)
		for _, groupAggObject := range groupMap {
			groupAgg := groupAggObject.Object.(AggregatorInterface)
			agg.Insert(groupAgg.One())
		}
		return agg.Result()
	} else {
		return a.List
	}
}

func (a *Aggregator) One() *pb.ScoredDatum {
	if len(a.List) > 0 {
		if a.Config.HigherIsBetter {
			sumScore := float64(0)
			for _, scored := range a.List {
				sumScore += scored.Score
			}
			return &pb.ScoredDatum{
				Score: sumScore,
				Datum: a.List[0].Datum,
			}
		} else {
			sumScore := float64(0)
			for _, scored := range a.List {
				sumScore += scored.Score
			}
			floatLen := float64(len(a.List))
			sumScore = sumScore / (floatLen * floatLen)
			return &pb.ScoredDatum{
				Score: sumScore,
				Datum: a.List[0].Datum,
			}
		}
	}
	return nil
}
