package data

import (
	pb "github.com/bgokden/veri/veriservice"
)

func isEvictionOn(localInfo *pb.DataInfo, config *pb.DataConfig, deleted uint64) bool {
	lowerThreshold := uint64(float64(localInfo.TargetN) * config.TargetUtilization) // This should be configurable
	if !config.NoTarget && (localInfo.N-deleted) >= lowerThreshold {
		return true
	}
	return false
}

// StreamCollector collects results
type StreamCollector struct {
	DatumStream chan<- *pb.Datum
}

// InsertStreamCollector collects results
type InsertStreamCollector struct {
	DatumStream chan<- *pb.InsertDatumWithConfig
}

func InsertConfigFromExpireAt(expiresAt uint64) *pb.InsertConfig {
	var timeLeftInSeconds uint64
	if expiresAt > 0 {
		timeLeftInSeconds = expiresAt - uint64(getCurrentTime())
	} else {
		timeLeftInSeconds = 0
	}
	return &pb.InsertConfig{
		TTL:   timeLeftInSeconds,
		Count: 1,
	}
}
