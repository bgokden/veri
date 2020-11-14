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
	Context          *pb.SearchContext
	ScoreFunc        func(arr1 []float64, arr2 []float64) float64
}

func NewAggrator(config *pb.SearchConfig, grouped bool, context *pb.SearchContext) AggregatorInterface {
	a := &Aggregator{
		Config:    config,
		Grouped:   grouped,
		Context:   context,
		ScoreFunc: GetVectorComparisonFunction(config.ScoreFuncName),
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

func (a *Aggregator) BestScore(scoredDatum *pb.ScoredDatum) float64 {
	if a.Context != nil || len(a.Context.GetDatum()) > 0 {
		var isSet = false
		var current float64
		// When Context is crioritized search score is ignored.
		if !a.Context.Prioritize {
			current = scoredDatum.GetScore()
			isSet = true
		}
		for _, datum := range a.Context.GetDatum() {
			newScore := a.ScoreFunc(scoredDatum.Datum.Key.Feature, datum.Key.Feature)
			if !isSet || a.IsNewScoredBetter(current, newScore) {
				current = newScore
				isSet = true
			}
		}

		return current
	}
	return scoredDatum.GetScore()
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
	scoredDatum.Score = a.BestScore(scoredDatum)
	if a.Grouped {
		keyString := string(scoredDatum.Datum.Key.GroupLabel)
		if aGroupAggregatorInterface, ok := a.DeDuplicationMap.Get(keyString); ok {
			aGroupAggregator := aGroupAggregatorInterface.(AggregatorInterface)
			return aGroupAggregator.Insert(scoredDatum)
		} else {
			aGroupAggregator := NewAggrator(a.Config, false, nil)
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
		agg := NewAggrator(a.Config, false, a.Context)
		for _, groupAggObject := range groupMap {
			groupAgg := groupAggObject.Object.(AggregatorInterface)
			agg.Insert(groupAgg.One())
		}
		return agg.Result()
	} else {
		if a.Config.ResultLimit > 0 && len(a.List) > int(a.Config.ResultLimit) {
			return a.List[:a.Config.ResultLimit]
		}
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
