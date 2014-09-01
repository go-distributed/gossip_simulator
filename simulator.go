package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

// Parameters
var (
	N         int
	Fanout    int
	Fanin     int
	DelayLow  int
	DelayHigh int
	DropRate  float64
	CrashRate float64
)

// Gloabals
var (
	GlobalView []*Node

	MakeUps       int32
	BreakUps      int32
	Runs          int32
	TotalReceived int32
	TotalCrashed  int32
	TotalMessage  int32
)

type Node struct {
	id              int
	fanout          int
	fanin           int
	received        bool
	crashed         bool
	recvMsgCh       chan bool
	makeUpCh        chan int
	breakUpCh       chan int
	needNewFriendCh chan bool

	friends []int
}

func NewNode(id, fanout, fanin int) *Node {
	return &Node{
		id:              id,
		recvMsgCh:       make(chan bool, 1024),
		makeUpCh:        make(chan int, 1024),
		breakUpCh:       make(chan int, 1024),
		needNewFriendCh: make(chan bool, 1024),

		fanout:  fanout,
		fanin:   fanin,
		friends: make([]int, 0, fanout),
	}
}

func (n *Node) Start() {
	atomic.AddInt32(&Runs, 1)
	for {
		select {
		case id := <-n.makeUpCh: // New friendship request.
			atomic.AddInt32(&MakeUps, 1)
			if len(n.friends) < n.fanin {
				n.friends = append(n.friends, id)
			} else {
				victimPos := rand.Intn(len(n.friends))
				victimId := n.friends[victimPos]
				n.Breakup(victimId)
				n.friends[victimPos] = id
			}
		case id := <-n.breakUpCh: // End friendship request.
			atomic.AddInt32(&BreakUps, 1)
			for i := range n.friends {
				if n.friends[i] == id {
					if len(n.friends) > n.fanout {
						// We have more friends than we need, so just remove one.
						// is enough, no need to make new friends.
						n.removeFriend(i)
						break
					}
					newFriendId := rand.Intn(len(GlobalView))
					for newFriendId == id || newFriendId == n.id {
						newFriendId = rand.Intn(len(GlobalView))
					}
					n.friends[i] = newFriendId
					n.Makeup(newFriendId)
					break
				}
			}
		case <-n.needNewFriendCh: // Keep making new friends if we haven't enough.
			if len(n.friends) < n.fanout {
				newFriendId := rand.Intn(len(GlobalView))
				if newFriendId == n.id {
					newFriendId = (newFriendId + 1) % len(GlobalView)
				}
				n.friends = append(n.friends, newFriendId)
				n.Makeup(newFriendId)
				if len(n.friends) < n.fanout {
					go func() { n.needNewFriendCh <- true }()
				}
			}
		case <-n.recvMsgCh: // Handle messages.
			if n.crashed { // Crashed machine doesn't forward any messages.
				continue
			}
			atomic.AddInt32(&TotalMessage, 1)
			if RandomCrash() {
				n.crashed = true
				atomic.AddInt32(&TotalCrashed, 1)
				continue
			}
			if n.received { // If already received the message, won't forward.
				continue
			}
			n.received = true
			atomic.AddInt32(&TotalReceived, 1)
			n.Broadcast()
		}
	}
}

func (n *Node) removeFriend(friendPos int) {
	f := make([]int, len(n.friends)-1)
	j := 0
	for i := range n.friends {
		if i == friendPos {
			continue
		}
		f[j] = n.friends[i]
		j++
	}
	n.friends = f
}

func (n *Node) Broadcast() {
	go func() {
		<-time.After(RandomNetworkDelay())
		for _, id := range n.friends {
			if !RandomDrop() {
				GlobalView[id].recvMsgCh <- true
			}
		}
	}()
}

func (n *Node) Breakup(id int) {
	go func() {
		<-time.After(RandomNetworkDelay())
		GlobalView[id].breakUpCh <- n.id
	}()
}

func (n *Node) Makeup(id int) {
	go func() {
		<-time.After(RandomNetworkDelay())
		GlobalView[id].makeUpCh <- n.id
	}()

}

func RandomNetworkDelay() time.Duration {
	return time.Millisecond * time.Duration(DelayLow+rand.Intn(DelayHigh-DelayLow))
}

// Returns true if drop, false if not.
func RandomDrop() bool {
	if rand.Intn(100) < int(DropRate*100) {
		return true
	}
	return false
}

// Returns true if crashed.
func RandomCrash() bool {
	if rand.Intn(100) < int(CrashRate*100) {
		return true
	}
	return false
}

func init() {
	flag.IntVar(&N, "n", 50000, "total number of nodes")
	flag.IntVar(&Fanout, "fanout", 5, "fanout")
	flag.IntVar(&Fanin, "fanin", Fanout+1, "fanin")
	flag.IntVar(&DelayLow, "delaylow", 10, "delay low (ms)")
	flag.IntVar(&DelayHigh, "delayhigh", 20, "delay high (ms)")
	flag.Float64Var(&DropRate, "droprate", 0.1, "message drop rate")
	flag.Float64Var(&CrashRate, "crashrate", 0.001, "machine crash rate")

	flag.Parse()

	fmt.Println("=== Parameters ===")
	flag.VisitAll(func(fl *flag.Flag) {
		fmt.Print(fl.Name, "=", fl.Value)
		if fl.Name == "delaylow" || fl.Name == "delayhigh" {
			fmt.Print("ms")
		}
		fmt.Println()
	})
}

func main() {
	GlobalView = make([]*Node, N)

	for i := range GlobalView {
		GlobalView[i] = NewNode(i, Fanout, Fanin)
	}

	for i := range GlobalView {
		GlobalView[i].needNewFriendCh <- true
		go GlobalView[i].Start()
	}

	fmt.Println("\n=== Constructing Overlay ===")

	startTime := time.Now()
	for {
		<-time.After(time.Millisecond * 10)
		makeup, breakup := atomic.LoadInt32(&MakeUps), atomic.LoadInt32(&BreakUps)

		runs := atomic.LoadInt32(&Runs)
		if makeup == 0 && breakup == 0 && runs == int32(N) {
			break
		}
		fmt.Println("break", breakup, "makeup", makeup, "elasped", time.Since(startTime))

		atomic.StoreInt32(&MakeUps, 0)
		atomic.StoreInt32(&BreakUps, 0)
	}
	fmt.Printf("--- Took %v to stabilize ---\n\n", time.Since(startTime))

	fmt.Println("=== Broadcast one message ===")

	startTime = time.Now()
	senderPos := rand.Intn(len(GlobalView))
	GlobalView[senderPos].Broadcast()

	for {
		<-time.After(time.Millisecond * 10)
		totalReceived := atomic.LoadInt32(&TotalReceived)
		percent := float32(totalReceived) / float32(N)
		fmt.Printf("%v%% covered, took %v\n", percent*100, time.Since(startTime))
		if percent >= 0.99 {
			break
		}
	}
	fmt.Printf("--- Took %v to get 99%% ---\n\n", time.Since(startTime))
	fmt.Println("Total message", TotalMessage, "Total Crashed", TotalCrashed)

}
