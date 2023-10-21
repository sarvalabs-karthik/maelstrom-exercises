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

// Efficient Broadcast solution, Part 2
// batch the requests as they come and run a go routine to send packets every 500 milliseconds once
// when packet comes from client for first time, it can be sent immediately

var maxRetry = 100
var readIDs = "readIDs"

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
	node               *maelstrom.Node
	unsentIDs          []float64
	unsentIDsLock      sync.RWMutex
	receivedIDs        []int
	receivedIDsSet     map[int]struct{}
	receivedIDsLock    sync.RWMutex
	receivedIDsSetLock sync.RWMutex
	topology           map[string][]string
	children           []string
	root               []string
}

func newNode(node *maelstrom.Node) *Node {
	return &Node{
		node:           node,
		receivedIDs:    make([]int, 0),
		receivedIDsSet: make(map[int]struct{}),
		unsentIDs:      make([]float64, 0),
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

	log.Println("root ", n.root)
	n.children = getNodesFromNumbers(findChildren(bst, getNumberFromNode(n.node.ID())))
	log.Println("childs ", n.children)
}

func (n *Node) addToReceivedIDsSet(val int) {
	n.receivedIDsSetLock.Lock()
	defer n.receivedIDsSetLock.Unlock()

	n.receivedIDsSet[val] = struct{}{}
}

func (n *Node) hasID(val int) bool {
	n.receivedIDsSetLock.RLock()
	defer n.receivedIDsSetLock.RUnlock()

	_, found := n.receivedIDsSet[val]

	return found
}

func (n *Node) addToReceivedIDs(val int) {
	n.receivedIDsLock.Lock()
	defer n.receivedIDsLock.Unlock()

	n.receivedIDs = append(n.receivedIDs, val)
}

func (n *Node) readReceivedIDS() []int {
	n.receivedIDsLock.RLock()
	defer n.receivedIDsLock.RUnlock()

	return n.receivedIDs
}

func (n *Node) addToUnsentIDs(val int) {
	n.unsentIDsLock.Lock()
	defer n.unsentIDsLock.Unlock()

	log.Println("ajinkya add to unsent ids ", val)
	n.unsentIDs = append(n.unsentIDs, float64(val))
}

// syncRPCBroadcastMultiplePacketInTree: all nodes should send messages to child nodes
// when msg comes from non-client then root shouldn't send them to children as non-client will send them anyway
func (n *Node) syncRPCBroadcastMultiplePacketInTree(body json.RawMessage) error {
	neighbours := n.children

	for _, neighbour := range neighbours {
		dest := neighbour

		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			for i := 0; i < maxRetry; i++ {
				_, err := n.node.SyncRPC(ctx, dest, body)
				if err != nil {
					time.Sleep(250 * time.Millisecond)

					continue
				}

				break
			}

			log.Printf("broadcast ok received from %v", dest)
		}()
	}

	return nil
}

// syncRPCBroadcastPacketInTree: all nodes should send messages to child nodes and also when nodes get msg from client,
// they should send it to root and its two child's, to decrease the latency by 1 step
// root cannot choose root and its children as neighbours when it received message from client
// when msg comes from non-client then root shouldn't send them to children as non-client will send them anyway
func (n *Node) syncRPCBroadcastPacketInTree(msg maelstrom.Message) error {
	neighbours := n.children

	if n.node.ID() != n.root[0] {
		neighbours = append(neighbours, n.root...)
	}

	for _, neighbour := range neighbours {
		dest := neighbour

		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			for i := 0; i < maxRetry; i++ {
				_, err := n.node.SyncRPC(ctx, dest, msg.Body)
				if err != nil {
					time.Sleep(500 * time.Millisecond)

					log.Println("send again ")
					continue
				}

				break
			}

			log.Printf("broadcast ok received from %v", dest)
		}()
	}

	return nil
}

//
//func (n *Node) syncRPCBroadcastPacketToNeighbours(value float64, body json.RawMessage) error {
//	if !n.hasID(int(value)) {
//		n.addToReceivedIDsSet(int(value))
//		n.addToReceivedIDs(int(value))
//
//		neighbours := n.topology[n.node.ID()]
//
//		var wg sync.WaitGroup
//		wg.Add(len(neighbours))
//
//		for _, neighbour := range neighbours {
//			dest := neighbour
//
//			go func() {
//				defer wg.Done()
//
//				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
//				defer cancel()
//
//				resp, err := n.node.SyncRPC(ctx, dest, body)
//				if err != nil || !checkIfMsgTypeMatches(resp.Body, "broadcast_ok") {
//					n.addMissingPacket(dest, value)
//				}
//
//				log.Printf("broadcast ok received from %v", dest)
//			}()
//		}
//
//		wg.Wait()
//
//	}
//
//	return nil
//}

// broadcastPacketToNeighbours broadcasts packet only once to neighbours
func (n *Node) broadcastPacketToNeighbours(value float64, body json.RawMessage) error {
	_, found := n.receivedIDsSet[int(value)]
	if !found {
		n.receivedIDsSet[int(value)] = struct{}{}
		n.receivedIDs = append(n.receivedIDs, int(value))

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

func (n *Node) SendUnsentPackets() error {
	n.unsentIDsLock.Lock()
	defer n.unsentIDsLock.Unlock()

	vals := n.unsentIDs

	res := make(map[string]any)

	res["type"] = "broadcast"
	res[readIDs] = vals

	rawBody, err := json.Marshal(res)
	if err != nil {
		return err
	}

	if err := n.syncRPCBroadcastMultiplePacketInTree(rawBody); err != nil {
		return err
	}

	n.unsentIDs = make([]float64, 0)

	return nil
}

func convertToINTList(b json.RawMessage) ([]int, error) {
	var body map[string]any

	err := json.Unmarshal(b, &body)
	if err != nil {
		return nil, err
	}

	val, ok := body[readIDs]
	if !ok {
		return nil, errors.New("message type doesn't exist")
	}

	vals, ok := val.([]interface{})
	if !ok {
		log.Panic("neighbours are not string list")
	}

	result := make([]int, 0, len(vals))

	for _, v := range vals {
		result = append(result, int(v.(float64)))
	}

	return result, nil
}

func main() {
	node := maelstrom.NewNode()
	customNode := newNode(node)

	go func() {
		for {
			select {
			case <-time.After(500 * time.Millisecond):
				customNode.SendUnsentPackets()
			}
		}
	}()

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		if err := customNode.acknowledgeBroadcastPacket(msg); err != nil {
			return err
		}

		_, ok := body[readIDs]
		if !ok {
			val, ok := body["message"]
			if !ok {
				return errors.New("message type doesn't exist")
			}

			value, ok := val.(float64)
			if !ok {
				return errors.New("not a float64")
			}

			if !customNode.hasID(int(value)) {
				customNode.addToReceivedIDsSet(int(value))
				customNode.addToReceivedIDs(int(value))
			}

			body[readIDs] = []float64{value}

			rawBody, err := json.Marshal(body)
			if err != nil {
				return err
			}

			msg.Body = rawBody

			if err := customNode.syncRPCBroadcastPacketInTree(msg); err != nil {
				return err
			}

			return nil
		}

		values, err := convertToINTList(msg.Body)
		if err != nil {
			return err
		}

		for _, v := range values {
			if !customNode.hasID(v) {
				customNode.addToReceivedIDsSet(v)
				customNode.addToReceivedIDs(v)
				customNode.addToUnsentIDs(v)
			}
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
		respBody["messages"] = customNode.readReceivedIDS()

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
