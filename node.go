package main

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

var GlobalView []*Node

var MakeUps int32
var BreakUps int32
var Runs int32
var TotalReceived int32
var TotalCrashed int32
var TotalMessage int32

var DelayLow int
var DelayHigh int
var DropRate float32
var CrashRate float32

type Node struct {
	id              int
	fanout          int
	received        bool
	crashed         bool
	recvMsgCh       chan bool
	makeUpCh        chan int
	breakUpCh       chan int
	needNewFriendCh chan bool

	friends []int
}

func NewNode(id, fanout int) *Node {
	return &Node{
		id:              id,
		recvMsgCh:       make(chan bool, 1024),
		makeUpCh:        make(chan int, 1024),
		breakUpCh:       make(chan int, 1024),
		needNewFriendCh: make(chan bool, 1024),

		fanout:  fanout,
		friends: make([]int, 0, fanout),
	}
}

func (n *Node) Start() {
	atomic.AddInt32(&Runs, 1)
	for {
		select {
		case id := <-n.makeUpCh: // New friends request
			atomic.AddInt32(&MakeUps, 1)
			if len(n.friends) < n.fanout+2 {
				n.friends = append(n.friends, id)
			} else {
				victimPos := rand.Intn(len(n.friends))
				victimId := n.friends[victimPos]
				n.Breakup(victimId)
				n.friends[victimPos] = id
			}
		case id := <-n.breakUpCh:
			atomic.AddInt32(&BreakUps, 1)
			if len(n.friends) > n.fanout {
				continue
			}
			for i := range n.friends {
				if n.friends[i] == id {
					newFriendId := rand.Intn(len(GlobalView))
					for newFriendId == id || newFriendId == n.id {
						newFriendId = rand.Intn(len(GlobalView))
					}
					n.friends[i] = newFriendId
					n.Makeup(newFriendId)
				}
			}
		case <-n.needNewFriendCh:
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
		case <-n.recvMsgCh:
			if n.crashed {
				continue
			}
			atomic.AddInt32(&TotalMessage, 1)
			if RandomCrash() {
				n.crashed = true
				atomic.AddInt32(&TotalCrashed, 1)
				continue
			}
			if n.received {
				continue
			}
			n.received = true
			atomic.AddInt32(&TotalReceived, 1)
			n.Broadcast()
		}
	}
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

func SetDelay(low, high int) {
	DelayLow, DelayHigh = low, high
}

func SetDropRate(drop float32) {
	DropRate = drop
}

func SetCrashRate(crash float32) {
	CrashRate = crash
}

func main() {
	n, fanout := 50000, 5
	GlobalView = make([]*Node, n)

	// Setup Delay, DropRate and CrashRate
	SetDelay(10, 20) // ms
	SetDropRate(0.1)
	SetCrashRate(0.001) // 0.1% CrashRate

	for i := range GlobalView {
		GlobalView[i] = NewNode(i, fanout)
	}

	for i := range GlobalView {
		GlobalView[i].needNewFriendCh <- true
		go GlobalView[i].Start()
	}

	startTime := time.Now()
	for {
		<-time.After(time.Microsecond * 100)
		makeup, breakup := atomic.LoadInt32(&MakeUps), atomic.LoadInt32(&BreakUps)

		runs := atomic.LoadInt32(&Runs)
		if makeup == 0 && breakup == 0 && runs == int32(n) {
			break
		}
		//fmt.Println("break", breakup, "makeup", makeup)

		atomic.StoreInt32(&MakeUps, 0)
		atomic.StoreInt32(&BreakUps, 0)
	}
	elasped := time.Since(startTime)
	//for i := range GlobalView {
	//	fmt.Println(i, ":", GlobalView[i].friends, len(GlobalView[i].friends))
	//
	//}
	fmt.Printf("Took %v to stablize\n", elasped)

	startTime = time.Now()

	senderPos := rand.Intn(len(GlobalView))

	GlobalView[senderPos].Broadcast()

	for {
		<-time.After(time.Millisecond * 10)
		totalReceived := atomic.LoadInt32(&TotalReceived)
		percent := float32(totalReceived) / float32(n)
		fmt.Printf("%v%% covered, took %v\n", percent*100, time.Since(startTime))
		if percent >= 0.99 {
			break
		}
	}
	elasped = time.Since(startTime)
	fmt.Printf("Took %v to get 99%%\n", elasped)
	fmt.Println("Total message", TotalMessage, "Total Crashed", TotalCrashed)

}
