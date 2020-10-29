package node

import (
	"log"
	"sort"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"

	data "github.com/bgoken/veri-data"
)

type Service struct {
}

type Peer struct {
	Version     string
	Timestamp   uint64
	Ids         []string
	ServiceList []string
	PeerList    []Peer
	DataList    []data.DataConfig
}

func (p *Peer) GetId() string {
	return SerializeStringArray(p.Ids)
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
	Version        string
	Port           uint32
	Folder         string
	KnownIds       []string
	AdvertisedIds  []string
	Dataset        *data.Dataset
	ServiceList    *cache.Cache
	PeerList       *cache.Cache
	PeriodicTicker *time.Ticker
	PeriodicDone   chan bool
}

func NewNode(config *NodeConfig) *Node {
	node := &Node{}
	node.Port = config.Port
	node.Folder = config.Folder
	node.AdvertisedIds = config.AdvertisedIds
	node.Dataset = data.NewDataset(node.Folder)
	node.PeerList = cache.New(5*time.Minute, 10*time.Minute)
	node.ServiceList = cache.New(5*time.Minute, 10*time.Minute)
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

func (n *Node) AddService(service string) error {
	n.ServiceList.Add(service, true, cache.DefaultExpiration)
	return nil
}

func (n *Node) AddPeer(peer *Peer) error {
	if !n.isPeerSimilarToNode(peer.Ids) {
		n.PeerList.Set(peer.GetId(), peer, cache.DefaultExpiration)
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

func (n *Node) PeerListItems() []Peer {
	peerList := n.PeerList.Items()
	items := make([]Peer, 0, len(peerList))
	for _, itemObject := range peerList {
		item := itemObject.Object.(*Peer)
		items = append(items, *item)
	}
	return items
}

func (n *Node) GetNodeInfo() *Peer {
	ids := make([]string, 0)
	ids = append(ids, n.KnownIds...)
	ids = append(ids, n.AdvertisedIds...)
	p := &Peer{
		Version:     n.Version,
		Timestamp:   getCurrentTime(),
		Ids:         ids,
		ServiceList: n.ServiceListKeys(),
		PeerList:    n.PeerListItems(),
		DataList:    n.Dataset.DataConfigList(),
	}
	return p
}

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

func (n *Node) isPeerSimilarToNode(ids []string) bool {
	return checkSimilar(ids, n.KnownIds) || checkSimilar(ids, n.AdvertisedIds)
}

func (n *Node) SyncWithPeers() {
	peerList := n.PeerList.Items()
	for _, item := range peerList {
		peer := item.Object.(*Peer)
		for _, serviceFromPeer := range peer.ServiceList {
			n.AddService(serviceFromPeer)
		}
		for _, peerFromPeer := range peer.PeerList {
			n.AddPeer(&peerFromPeer)
		}
		for _, dataConfigFromPeer := range peer.DataList {
			data, _ := n.Dataset.GetOrCreateIfNotExists(&dataConfigFromPeer)
			data.AddSource(peer.GetDataSourceClient(dataConfigFromPeer.Name))
		}
	}
}

func (n *Node) JoinToPeers() error {
	serviceList := n.ServiceList.Items()
	for id := range serviceList {
		n.SendJoinRequest(id)
	}
	peerList := n.PeerList.Items()
	for _, item := range peerList {
		peer := item.Object.(*Peer)
		for _, id := range peer.Ids {
			n.SendJoinRequest(id)
		}
	}
	return nil
}

func (n *Node) SetPeriodicTask() {
	n.PeriodicTicker = time.NewTicker(10 * time.Second)
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
	go n.JoinToPeers()
	go n.SyncWithPeers()
	return nil
}
