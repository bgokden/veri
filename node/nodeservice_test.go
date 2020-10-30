package node_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	node "github.com/bgokden/veri-node"
)

func randomPort() uint32 {
	portMin := 5000
	portMax := 6000
	return uint32(rand.Intn(portMax-portMin+1) + portMin)
}

func TempNode(serviceAddress string) *node.Node {
	dir0, err := ioutil.TempDir("tmp", "node")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir0)

	portNumber0 := randomPort()
	broadcastAddresses0 := []string{fmt.Sprintf("localhost:%d", portNumber0)}
	serviceAddresses0 := []string{}
	if serviceAddress != "" {
		serviceAddresses0 = append(serviceAddresses0, serviceAddress)
	}
	nodeConfig0 := &node.NodeConfig{
		Port:          portNumber0,
		Folder:        dir0,
		AdvertisedIds: broadcastAddresses0,
		ServiceList:   serviceAddresses0,
	}

	log.Printf("conf: %v", nodeConfig0)
	node0 := node.NewNode(nodeConfig0)

	go node0.Listen()

	return node0
}

func TestNode(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	node0 := TempNode("")
	defer os.RemoveAll(node0.Folder)

	service0 := node0.AdvertisedIds[0]
	node1 := TempNode(service0)
	defer os.RemoveAll(node1.Folder)

	node2 := TempNode(service0)
	defer os.RemoveAll(node2.Folder)

	time.Sleep(30 * time.Second)

	log.Printf("Peer of peer0:\n")
	for _, peerFromPeer := range node0.PeerListItems() {
		log.Printf("%v:\n", peerFromPeer.AddressList)
	}

	log.Printf("Peer of peer1:\n")
	for _, peerFromPeer := range node1.PeerListItems() {
		log.Printf("%v:\n", peerFromPeer.AddressList)
	}
}
