package main

import (
	"context"
	"encoding/json"
	"errors"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

// multinode broadcast solution
// 1. whenever broadcast message received
// 2. check if node received packet already then don't broadcast or reply to it
// 3. else send packet to all other nodes, reply to the broadcast packet with broadcast_ok message as it would have come from worker
// optimization that are made:
// avoided broadcast flooding
// optimization that can be made:
// 1. do not send packet if destination already has it

// Fault-tolerant broadcast solution
// lets say communication is down between node n1 and n2
// n1 can keep track of broadcast messages for which reply is not received
// and retry sending packets every 1 second (use a goroutine which handles this

// Efficient Broadcast solution , Part I
//
//	1 (square topology) send packet only to neighbours in topology (2D manner), so that no of packets in network can be reduced but this increases latency
//	maximum distance between any two nodes can be 2*sqrt(n)
//	as diagonal length is sqrt(n) in a square but we travel only in 2D graph so it is 2*sqrt(n)
//
// 2. In tree topology maximum distance for a packet to travel will be log(n) as every node will have its child's and root as neighbours
// no of packets emitted per operation will be n, if a packet coming from tree node then don't send the packet to root and its two child's. else send to root if coming from client
// build the binary tree initially to identify neighbours

func checkIfMsgTypeMatches(msgBody json.RawMessage, expectedType string) bool {
	var body map[string]any

	err := json.Unmarshal(msgBody, &body)
	if err != nil {
		return false
	}

	value, ok := body["type"]
	if !ok {
		return false
	}

	actualType, ok := value.(string)
	if !ok {
		return false
	}

	if expectedType != actualType {
		return false
	}

	return true
}

type N struct {
	left  *N
	right *N
	data  int
}

func getBST(n []int) *N {
	if len(n) == 0 {
		return nil
	}
	mid := len(n) / 2

	node := &N{}

	node.left = getBST(n[:mid])
	node.right = getBST(n[mid+1:])
	node.data = n[mid]

	return node
}

func findChildren(tree *N, n int) []int {
	if n == tree.data {
		childs := make([]int, 0)

		if tree.left != nil {
			childs = append(childs, tree.left.data)
		}

		if tree.right != nil {
			childs = append(childs, tree.right.data)
		}

		return childs
	}

	if n > tree.data {
		return findChildren(tree.right, n)
	}

	return findChildren(tree.left, n)
}

func ISClient(n string) bool {
	return !strings.HasPrefix(n, "n")
}

func getNodeFromNumber(n int) string {
	return "n" + strconv.Itoa(n)
}

func getNumberFromNode(node string) int {
	num := strings.TrimPrefix(node, "n")
	val, err := strconv.Atoi(num)
	if err != nil {
		panic(err)
	}

	return val
}

func getNodesFromNumbers(numbers []int) []string {
	childs := make([]string, 0)

	for _, n := range numbers {
		childs = append(childs, getNodeFromNumber(n))
	}

	return childs
}

type Node struct {
	node           *maelstrom.Node
	messages       []int
	messageSet     map[int]struct{}
	missingPackets map[string]map[float64]struct{} // map of node to array of packet to be resent at regular intervals
	missingLock    sync.Mutex
	msgLock        sync.RWMutex
	messageSetLock sync.RWMutex
	topology       map[string][]string
	children       []string
	root           []string
}

func newNode(node *maelstrom.Node) *Node {
	return &Node{
		node:           node,
		messages:       make([]int, 0),
		messageSet:     make(map[int]struct{}),
		missingPackets: make(map[string]map[float64]struct{}),
		topology:       make(map[string][]string),
		root:           make([]string, 0),
	}
}

func (n *Node) initializeTopology(t any) {
	topology, ok := t.(map[string]any)
	if !ok {
		log.Panic("neighbours are not expected type (map[string]any)")
	}

	for src, neighbours := range topology {
		neighbours, ok := neighbours.([]interface{})
		if !ok {
			log.Panic("neighbours are not string list")
		}

		neigh := make([]string, 0, len(neighbours))

		for _, node := range neighbours {
			neigh = append(neigh, node.(string))
		}

		n.topology[src] = neigh
	}

	nodes := n.node.NodeIDs()

	nodeList := make([]int, len(nodes))

	for i := 0; i < len(nodes); i++ {
		nodeList[i] = i
	}

	bst := getBST(nodeList)
	n.root = append(n.root, getNodeFromNumber(bst.data))
	n.root = append(n.root, getNodeFromNumber(bst.right.data))
	n.root = append(n.root, getNodeFromNumber(bst.left.data))

	n.children = getNodesFromNumbers(findChildren(bst, getNumberFromNode(n.node.ID())))
}

func (n *Node) addToMessageSet(val int) {
	n.messageSetLock.Lock()
	defer n.messageSetLock.Unlock()

	n.messageSet[val] = struct{}{}
}

func (n *Node) hasMessage(val int) bool {
	n.messageSetLock.RLock()
	defer n.messageSetLock.RUnlock()

	_, found := n.messageSet[val]

	return found
}

func (n *Node) addToMessages(val int) {
	n.msgLock.Lock()
	defer n.msgLock.Unlock()

	n.messages = append(n.messages, val)
}

func (n *Node) readMessages() []int {
	n.msgLock.RLock()
	defer n.msgLock.RUnlock()

	return n.messages
}

func (n *Node) addMissingPacket(neighbour string, value float64) {
	n.missingLock.Lock()
	defer n.missingLock.Unlock()

	log.Printf("add missing packet %s, %v, %v ", neighbour, value, len(n.missingPackets))

	if _, ok := n.missingPackets[neighbour]; !ok {
		n.missingPackets[neighbour] = make(map[float64]struct{})
	}

	n.missingPackets[neighbour][value] = struct{}{}
}

func (n *Node) deleteMissingPacket(neighbour string, packet float64) {
	n.missingLock.Lock()
	defer n.missingLock.Unlock()

	delete(n.missingPackets[neighbour], packet)

	if len(n.missingPackets[neighbour]) == 0 {
		delete(n.missingPackets, neighbour)
	}
}

// syncRPCBroadcastPacketInTree: all nodes should send messages to child nodes and also when nodes get msg from client,
// they should send it to root and its two child's, to decrease the latency by 1 step
// root cannot choose root and its children as neighbours when it received message from client
// when msg comes from non-client then root shouldn't send them to children as non-client will send them anyway
func (n *Node) syncRPCBroadcastPacketInTree(value float64, msg maelstrom.Message) error {
	if !n.hasMessage(int(value)) {
		n.addToMessageSet(int(value))
		n.addToMessages(int(value))

		if n.node.ID() == n.root[0] && !ISClient(msg.Src) {
			return nil
		}

		neighbours := n.children

		if ISClient(msg.Src) && n.node.ID() != n.root[0] {
			neighbours = append(neighbours, n.root...)
			neighbours = append(neighbours, n.root...)
		}

		var wg sync.WaitGroup
		wg.Add(len(neighbours))

		for _, neighbour := range neighbours {
			dest := neighbour

			go func() {
				defer wg.Done()

				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()

				resp, err := n.node.SyncRPC(ctx, dest, msg.Body)
				if err != nil || !checkIfMsgTypeMatches(resp.Body, "broadcast_ok") {
					n.addMissingPacket(dest, value)
				}

				log.Printf("broadcast ok received from %v", dest)
			}()
		}

		wg.Wait()

	}

	return nil
}

func (n *Node) syncRPCBroadcastPacketToNeighbours(value float64, body json.RawMessage) error {
	if !n.hasMessage(int(value)) {
		n.addToMessageSet(int(value))
		n.addToMessages(int(value))

		neighbours := n.topology[n.node.ID()]

		var wg sync.WaitGroup
		wg.Add(len(neighbours))

		for _, neighbour := range neighbours {
			dest := neighbour

			go func() {
				defer wg.Done()

				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()

				resp, err := n.node.SyncRPC(ctx, dest, body)
				if err != nil || !checkIfMsgTypeMatches(resp.Body, "broadcast_ok") {
					n.addMissingPacket(dest, value)
				}

				log.Printf("broadcast ok received from %v", dest)
			}()
		}

		wg.Wait()

	}

	return nil
}

func (n *Node) sendMissingPackets() {
	log.Printf("send missing packets", len(n.missingPackets))

	var missingGroup sync.WaitGroup
	missingGroup.Add(len(n.missingPackets))

	for neigh, packets := range n.missingPackets {
		neighbour := neigh
		packets := packets

		go func() {
			defer missingGroup.Done()

			var packetWaitGroup sync.WaitGroup
			packetWaitGroup.Add(len(packets))

			for packet, _ := range packets {
				packet := packet

				go func() {
					defer packetWaitGroup.Done()

					body := make(map[string]any)
					body["type"] = "broadcast"
					body["message"] = packet

					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					defer cancel()

					resp, err := n.node.SyncRPC(ctx, neighbour, body)
					if err != nil || !checkIfMsgTypeMatches(resp.Body, "broadcast_ok") {
						return
					}

					n.deleteMissingPacket(neighbour, packet)
				}()

			}

			packetWaitGroup.Wait()

		}()

	}

	missingGroup.Wait()
}

// broadcastPacketToNeighbours broadcasts packet only once to neighbours
func (n *Node) broadcastPacketToNeighbours(value float64, body json.RawMessage) error {
	_, found := n.messageSet[int(value)]
	if !found {
		n.messageSet[int(value)] = struct{}{}
		n.messages = append(n.messages, int(value))

		neighbours := n.node.NodeIDs()

		for _, neighbour := range neighbours {
			if err := n.node.Send(neighbour, body); err != nil {
				return err
			}
		}
	}

	return nil
}

// acknowledgeBroadcastPacket replies with broadcast_ok
func (n *Node) acknowledgeBroadcastPacket(msg maelstrom.Message) error {
	// acknowledge that broadcast packet is received
	respBody := make(map[string]any)
	respBody["type"] = "broadcast_ok"

	if err := n.node.Reply(msg, respBody); err != nil {
		return err
	}

	return nil
}

func main() {
	node := maelstrom.NewNode()
	customNode := newNode(node)

	go func() {
		for {
			select {
			case <-time.After(1 * time.Second):
				customNode.sendMissingPackets()
			}
		}
	}()

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		val, ok := body["message"]
		if !ok {
			return errors.New("message type doesn't exist")
		}

		value, ok := val.(float64)
		if !ok {
			return errors.New("not a float64")
		}

		if err := customNode.acknowledgeBroadcastPacket(msg); err != nil {
			return err
		}

		if err := customNode.syncRPCBroadcastPacketInTree(value, msg); err != nil {
			return err
		}

		return nil
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		respBody := make(map[string]any)

		respBody["type"] = "read_ok"
		respBody["messages"] = customNode.readMessages()

		if err := node.Reply(msg, respBody); err != nil {
			return err
		}

		return nil
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		respBody := make(map[string]any)

		respBody["type"] = "topology_ok"

		if err := node.Reply(msg, respBody); err != nil {
			return err
		}

		t, ok := body["topology"]
		if !ok {
			return errors.New("topology not found")
		}

		customNode.initializeTopology(t)

		return nil
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
