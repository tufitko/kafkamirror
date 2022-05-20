package stream_test

import (
	"testing"

	"github.com/tufitko/kafkamirror/pkg/stream"
)

func TestPool(t *testing.T) {
	p := stream.NewPool(3, 5)

	// map[partID][message]bool
	cases := map[int]map[*stream.Message]bool{
		0: {
			{Key: "0"}: true,
		},
		1: {
			{Key: "1"}: true,
			{Key: "2"}: true,
			{Key: "4"}: true,
		},
		2: {
			{Key: "00"}: true,
			{Key: "3"}:  true,
			{Key: "6"}:  true,
			{Key: "5"}:  true,
		},
	}

	for _, msgList := range cases {
		for msg := range msgList {
			p.Add(msg)
		}
	}

outer:
	for pid, msgMap := range cases {
		handled := 0
		for msg := range p.Partition(pid) {
			if !msgMap[msg] {
				t.Errorf("message is expected to be in partition %d", pid)
			}
			handled++

			if handled == len(msgMap) {
				continue outer
			}
		}
	}
}
