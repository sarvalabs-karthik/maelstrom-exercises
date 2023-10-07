package main

import (
	"context"
	"encoding/json"
	"errors"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
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
// 2. send packet only to neighbours in topology

// Fault-tolerant broadcast solution
// lets say communication is down between node n1 and n2
// n1 can keep track of broadcast messages for which reply is not received
// and retry sending packets every 1 second (use a goroutine which handles this

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

type Node struct {
	node           *maelstrom.Node
	messages       []int
	messageSet     map[int]struct{}
	missingPackets map[string]map[float64]struct{} // map of node to array of packet to be resent at regular intervals
	mtx            sync.Mutex
}

func newNode(node *maelstrom.Node) *Node {
	return &Node{
		node:           node,
		messages:       make([]int, 0),
		messageSet:     make(map[int]struct{}),
		missingPackets: make(map[string]map[float64]struct{}),
	}
}

func (n *Node) addMissingPacket(neighbour string, value float64) {
	log.Printf("add missing packet %s, %v, %v ", neighbour, value, len(n.missingPackets))
	if _, ok := n.missingPackets[neighbour]; !ok {
		n.missingPackets[neighbour] = make(map[float64]struct{})
	}

	n.missingPackets[neighbour][value] = struct{}{}
}

func (n *Node) deleteMissingPacket(neighbour string, packet float64) {
	delete(n.missingPackets[neighbour], packet)

	if len(n.missingPackets[neighbour]) == 0 {
		delete(n.missingPackets, neighbour)
	}
}

func (n *Node) syncRPCBroadcastPacketToNeighbours(value float64, body json.RawMessage) error {
	_, found := n.messageSet[int(value)]
	if !found {
		n.messageSet[int(value)] = struct{}{}
		n.messages = append(n.messages, int(value))

		neighbours := n.node.NodeIDs()

		for _, neighbour := range neighbours {
			if n.node.ID() != neighbour {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				resp, err := n.node.SyncRPC(ctx, neighbour, body)
				if err != nil {
					n.addMissingPacket(neighbour, value)
					cancel()

					continue
				}

				if !checkIfMsgTypeMatches(resp.Body, "broadcast_ok") {
					n.addMissingPacket(neighbour, value)
					cancel()

					continue
				}

				cancel()

				log.Printf("broadcast ok received from %v", neighbour)
			}
		}
	}

	return nil
}

func (n *Node) sendMissingPackets() {
	body := make(map[string]any)
	body["type"] = "broadcast"

	log.Printf("send missing packets", len(n.missingPackets))
	for neighbour, packets := range n.missingPackets {
		for packet, _ := range packets {
			body["message"] = packet

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			resp, err := n.node.SyncRPC(ctx, neighbour, body)
			if err != nil {
				cancel()

				continue
			}

			if !checkIfMsgTypeMatches(resp.Body, "broadcast_ok") {
				cancel()

				continue
			}

			cancel()

			n.deleteMissingPacket(neighbour, packet)
		}
	}
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

		if err := customNode.syncRPCBroadcastPacketToNeighbours(value, msg.Body); err != nil {
			return err
		}

		if err := customNode.acknowledgeBroadcastPacket(msg); err != nil {
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
		respBody["messages"] = customNode.messages

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

		return nil
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
