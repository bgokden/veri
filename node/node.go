package node

import (
	"fmt"
	"log"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/bgokden/go-cache"
	"github.com/bgokden/veri/util"
	pb "github.com/bgokden/veri/veriservice"

	"github.com/bgokden/veri/state"

	data "github.com/bgokden/veri/data"
)

const MINGOMAXPROCS = 128

func GetIdOfPeer(p *pb.Peer) string {
	return SerializeStringArray(p.GetAddressList())
}

func SerializeStringArray(list []string) string {
	sort.Strings(list)
	return strings.Join(list[:], ",")
}

type NodeConfig struct {
	Version       string
	Port          uint32
	Folder        string
	AdvertisedIds []string
	ServiceList   []string
}

type Node struct {
	Version         string
	Port            uint32
	Folder          string
	KnownIds        []string
	AdvertisedIds   []string
	Dataset         *data.Dataset
	ServiceList     *cache.Cache
	PeerList        *cache.Cache
	PeriodicTicker  *time.Ticker
	PeriodicDone    chan bool
	QueryUUIDCache  *cache.Cache
	ConnectionCache *util.ConnectionCache
}

func NewNode(config *NodeConfig) *Node {
	node := &Node{}
	node.Port = config.Port
	node.Folder = config.Folder
	node.AdvertisedIds = config.AdvertisedIds
	node.Dataset = data.NewDataset(node.Folder)
	node.PeerList = cache.New(5*time.Minute, 1*time.Minute)
	node.ServiceList = cache.New(5*time.Minute, 1*time.Minute)
	node.QueryUUIDCache = cache.New(5*time.Minute, 1*time.Minute)
	node.ConnectionCache = util.NewConnectionCache()
	for _, service := range config.ServiceList {
		node.AddStaticService(service)
	}
	go node.JoinToPeers()
	go node.SyncWithPeers()
	node.SetPeriodicTask()

	return node
}

func (n *Node) AddStaticService(service string) error {
	n.ServiceList.Set(service, true, cache.NoExpiration)
	return nil
}

func (n *Node) Close() error {
	state.Ready = false
	state.Drain = true
	n.Dataset.Close()
	log.Printf("Graceful close.")
	return nil
}
func (n *Node) AddService(service string) error {
	n.ServiceList.Add(service, true, cache.DefaultExpiration)
	n.ServiceList.IncrementExpiration(service, 10*time.Minute)
	return nil
}

func IsRecent(timestamp uint64) bool {
	return timestamp+300 > getCurrentTime()
}

func (n *Node) CheckPeer(peer *pb.Peer) bool {
	if !n.isPeerSimilarToNode(peer) {
		if IsRecent(peer.GetTimestamp()) {
			return true
		}
		idOfPeer := GetIdOfPeer(peer)
		log.Printf("Deleting %v since %v is too old (t:%v)\n", idOfPeer, peer.Timestamp, getCurrentTime())
		n.PeerList.Delete(idOfPeer)
	}
	return false
}

func (n *Node) AddPeerElement(peer *pb.Peer) error {
	if !n.isPeerSimilarToNode(peer) {
		idOfPeer := GetIdOfPeer(peer)
		if oldPeerInterface, ok := n.PeerList.Get(idOfPeer); ok {
			if oldPeer, ok2 := oldPeerInterface.(*pb.Peer); ok2 {
				if oldPeer.Timestamp >= peer.Timestamp {
					// New one is older than current peer info
					return nil
				}
			}
		}
		if IsRecent(peer.GetTimestamp()) {
			n.PeerList.Set(idOfPeer, peer, cache.DefaultExpiration)
			n.PeerList.IncrementExpiration(idOfPeer, 10*time.Minute)
		}
	}
	return nil
}

func getCurrentTime() uint64 {
	return uint64(time.Now().Unix())
}

func (n *Node) ServiceListKeys() []string {
	serviceList := n.ServiceList.Items()
	keys := make([]string, 0, len(serviceList))
	for k := range serviceList {
		keys = append(keys, k)
	}
	return keys
}

func (n *Node) PeerListItems() []*pb.Peer {
	peerList := n.PeerList.Items()
	items := make([]*pb.Peer, 0, len(peerList))
	for _, itemObject := range peerList {
		item := itemObject.Object.(*pb.Peer)
		log.Printf("Peer: %v\n", item)
		items = append(items, item)
	}
	return items
}

func unique(strSlice []string) []string {
	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range strSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

func (n *Node) GetNodeInfo() *pb.Peer {
	ids := make([]string, 0)
	ids = append(ids, n.KnownIds...)
	ids = append(ids, n.AdvertisedIds...)
	ids = unique(ids)
	p := &pb.Peer{
		Version:     n.Version,
		Timestamp:   getCurrentTime(),
		AddressList: ids,
		ServiceList: n.ServiceListKeys(),
		DataList:    n.Dataset.DataConfigList(),
	}
	return p
}

// This is not working as it supposed to
func checkSimilar(list0, list1 []string) bool {
	for _, e0 := range list0 {
		for _, e1 := range list1 {
			if e0 == e1 {
				return true
			}
		}
	}
	return false
}

func FirstDifferent(list0, list1 []string) string {
	for _, e0 := range list1 {
		found := false
		for _, e1 := range list0 {
			if e0 == e1 {
				found = true
				break
			}
		}
		if !found {
			return e0
		}
	}
	return ""
}

func (n *Node) isPeerSimilarToNode(peer *pb.Peer) bool {
	id0 := GetIdOfPeer(n.GetNodeInfo())
	id1 := GetIdOfPeer(peer)
	return id0 == id1
	// return checkSimilar(ids, n.KnownIds) || checkSimilar(ids, n.AdvertisedIds)
}

func (n *Node) GetDifferentAddressOf(peer *pb.Peer) string {
	aList := n.GetNodeInfo().GetAddressList()
	return FirstDifferent(aList, peer.GetAddressList())
}

func (n *Node) SyncWithPeers() {
	// nodeId := GetIdOfPeer(n.GetNodeInfo())
	// log.Printf("(0) Node: %v\n", nodeId)
	// 	peerListPred := n.PeerList.Items()
	// 	deleteIDList := []string{}
	// 	for _, item := range peerListPred {
	// 		peer := item.Object.(*pb.Peer)
	// 		// Delete by timeout
	// 		if !IsRecent(peer.Timestamp) {
	// 			id := GetIdOfPeer(peer)
	// 			deleteIDList = append(deleteIDList, id)
	// 		}
	// 	}
	// 	for _, id := range deleteIDList {
	// 		log.Printf("Deleting Peer id: %v\n", id)
	// 		n.PeerList.Delete(id)
	// 	}
	peerList := n.PeerList.Items()
	for _, item := range peerList {
		peer := item.Object.(*pb.Peer)
		// Delete by timeout
		if !n.CheckPeer(peer) { // remove old peer
			continue
		}
		idOfPeer := n.GetDifferentAddressOf(peer)
		if idOfPeer == "" {
			continue
		}
		// log.Printf("(1) Node: %v -> Peer ID: %v\n", nodeId, idOfPeer)
		for _, serviceFromPeer := range peer.ServiceList {
			n.AddService(serviceFromPeer)
		}
		for _, itemOfPeer := range peerList {
			peerOfPeer := itemOfPeer.Object.(*pb.Peer)
			n.SendAddPeerRequest(idOfPeer, peerOfPeer)
		}
		for _, dataConfigFromPeer := range peer.DataList {
			data, err := n.Dataset.GetOrCreateIfNotExists(dataConfigFromPeer)
			// log.Printf("(2) dataN: %v peer %v dataConfigFromPeer %v idOfPeer %v\n", data.N, peer, dataConfigFromPeer, idOfPeer)
			if err == nil {
				data.AddSource(GetDataSourceClient(peer, dataConfigFromPeer.Name, idOfPeer, n.ConnectionCache))
			} else {
				log.Printf("Error data creation: %v\n", err)
			}
		}
	}
	state.Ready = true
	log.Printf("Synced with peers")
	// fmt.Println(n.Info())
}

func Find(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func (n *Node) JoinToPeers() error {
	idList := n.GetNodeInfo().GetAddressList()
	serviceList := n.ServiceList.Items()
	for id := range serviceList {
		n.SendJoinRequest(id)
	}
	peerList := n.PeerList.Items()
	for _, item := range peerList {
		peer := item.Object.(*pb.Peer)
		for _, id := range peer.AddressList {
			if !Find(idList, id) {
				n.SendJoinRequest(id)
			}
		}
	}
	return nil
}

func (n *Node) SetPeriodicTask() {
	n.PeriodicTicker = time.NewTicker(60 * time.Second)
	n.PeriodicDone = make(chan bool)
	go func() {
		for {
			select {
			case <-n.PeriodicDone:
				return
			case t := <-n.PeriodicTicker.C:
				err := n.Periodic()
				if err != nil {
					log.Println("Tick at", t)
					log.Printf("Periodic Task Error: %v\n", err)
				}
			}
		}
	}()

}

func (n *Node) StopPeriodicTask() {
	if n.PeriodicTicker != nil {
		n.PeriodicTicker.Stop()
	}
	n.PeriodicDone <- true
}

func (n *Node) Periodic() error {
	fmt.Println(n.Info())
	go n.JoinToPeers()
	go n.SyncWithPeers()
	go n.Dataset.ReloadRuns()
	go n.Dataset.SaveIndex()
	return nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (n *Node) Info() string {
	var sb strings.Builder
	nodeId := GetIdOfPeer(n.GetNodeInfo())
	sb.WriteString("-------------------------------------------------\n")
	gonumroutines := runtime.NumGoroutine()
	goMaxProcsHint := max(MINGOMAXPROCS, runtime.GOMAXPROCS(-1))
	sb.WriteString(fmt.Sprintf("-- Node ID: %v NUMGOROUTINES: %v GOMAXPROCS: %v\n", nodeId, gonumroutines, runtime.GOMAXPROCS(goMaxProcsHint)))
	sb.WriteString("DataList:\n")
	for _, name := range n.Dataset.List() {
		dt, err := n.Dataset.GetNoCreate(name)
		if err == nil {
			config := dt.GetConfig()
			dinfo := dt.GetDataInfo()
			sb.WriteString(fmt.Sprintf("* Name %v N: %v config %v\n", name, dinfo.N, config))
		} else {
			sb.WriteString(fmt.Sprintf("* Name %v Error: %v\n", name, err.Error()))
		}
		sourceList := dt.Sources.Items()
		for _, sourceItem := range sourceList {
			source := sourceItem.Object.(data.DataSource)
			sourceID := source.GetID()
			sourceInfo := source.GetDataInfo()
			if sourceInfo != nil {
				sb.WriteString(fmt.Sprintf("-- sourceID %v Version: %v N: %v\n", sourceID, sourceInfo.Version, sourceInfo.N))
			} else {
				sb.WriteString(fmt.Sprintf("-- sourceID %v Info not available\n", sourceID))
			}

		}
	}
	sb.WriteString("Peers:\n")
	peerList := n.PeerList.Items()
	for _, item := range peerList {
		peer := item.Object.(*pb.Peer)
		idOfPeer := GetIdOfPeer(peer)
		sb.WriteString(fmt.Sprintf("Peer: %v time-diff: %v\n", idOfPeer, getCurrentTime()-peer.Timestamp))
		sb.WriteString(fmt.Sprintf("DataList of Peer %v:\n", idOfPeer))
		for _, dataConfigFromPeer := range peer.DataList {
			sb.WriteString(fmt.Sprintf("* Name %v Version: %v dataConfigFromPeer: %v\n", dataConfigFromPeer.Name, dataConfigFromPeer.Version, dataConfigFromPeer))
		}
	}
	sb.WriteString("-------------------------------------------------\n")
	return sb.String()
}
