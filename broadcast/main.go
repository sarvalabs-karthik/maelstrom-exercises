package main

import (
	"encoding/json"
	"errors"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
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

func main() {
	node := maelstrom.NewNode()
	messages := make([]int, 0)
	set := make(map[int]struct{})

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		value, ok := body["message"].(float64)
		if !ok {
			return errors.New("not a float64")
		}

		respBody := make(map[string]any)

		_, found := set[int(value)]
		if !found {
			set[int(value)] = struct{}{}
			messages = append(messages, int(value))
			respBody["type"] = "broadcast_ok"

			if err := node.Reply(msg, respBody); err != nil {
				return err
			}

			neighbours := node.NodeIDs()

			for _, neighbour := range neighbours {
				if err := node.Send(neighbour, msg.Body); err != nil {
					return err
				}
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
		respBody["messages"] = messages

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
