package main

import (
	"sync"
)

type SafeCounter struct {
	mu sync.Mutex
	v  int
}

var lists = make(map[string][]string)
var blopSubscribers = make(map[string][]chan<- string)

type ReadReq struct {
	block bool
	key   string
	c     chan string
}

type WriteReq struct {
	key  string
	left bool
	val  string
}

var writeChan = make(chan WriteReq)
var readChan = make(chan ReadReq)

func listBroker(w <-chan WriteReq, r <-chan ReadReq) {
	for {
		select {
		case write := <-w:
			if len(blopSubscribers[write.key]) > 0 {
				sub := blopSubscribers[write.key][0]
				blopSubscribers[write.key] = blopSubscribers[write.key][1:]
				sub <- parseStringToRESP(write.val)
			} else {
				if write.left {
					lists[write.key] = append([]string{write.val}, lists[write.key]...)
				} else {
					lists[write.key] = append(lists[write.key], write.val)
				}
			}
		case read := <-r:
			if read.block {
				if len(lists[read.key]) > 0 {
					popped := lists[read.key][0]
					lists[read.key] = lists[read.key][1:]
					read.c <- parseStringToRESP(popped)
				} else {
					blopSubscribers[read.key] = append(blopSubscribers[read.key], read.c)
				}
			} else {
				if len(lists[read.key]) > 0 {
					popped := lists[read.key][0]
					lists[read.key] = lists[read.key][1:]
					read.c <- parseStringToRESP(popped)
				} else {
					read.c <- NULLBULK
				}
			}
		}
	}
}

// Inc increments the counter for the given key.
func (c *SafeCounter) Inc() {
	c.mu.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	c.v++
	c.mu.Unlock()
}

func (c *SafeCounter) Reset() {
	c.mu.Lock()
	c.v = 0
	c.mu.Unlock()
}

// Value returns the current value of the counter for the given key.
func (c *SafeCounter) Value() int {
	c.mu.Lock()
	// Lock so only one goroutine at a time can access the map c.v.
	defer c.mu.Unlock()
	return c.v
}

func (c *SafeCounter) IsEnough(v int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.v >= v
}
