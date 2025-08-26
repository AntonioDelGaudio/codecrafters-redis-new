package main

import (
	"fmt"
	"sync"
	"time"
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
	ret  chan int
}

var writeChan = make(chan WriteReq)
var readChan = make(chan ReadReq)

func listBroker(w <-chan WriteReq, r <-chan ReadReq) {
	for {
		select {
		case read := <-r:
			if read.block {
				if len(lists[read.key]) > 0 {
					fmt.Println("Element found, not blocking")
					popped := lists[read.key][0]
					lists[read.key] = lists[read.key][1:]
					read.c <- parseStringToRESP(popped)
				} else {
					fmt.Println("No element found, blocking")
					blopSubscribers[read.key] = append(blopSubscribers[read.key], read.c)
				}
			} else {
				if len(lists[read.key]) > 0 {
					fmt.Println("Element found, not blocking in non-blocking read")
					popped := lists[read.key][0]
					lists[read.key] = lists[read.key][1:]
					read.c <- parseStringToRESP(popped)
				} else {
					fmt.Println("No element found, returning nil in non-blocking read")
					read.c <- NULLBULK
				}
			}
		case write := <-w:
			fmt.Println("Waiting to see if there are subscribers")
			time.Sleep(time.Duration(10) * time.Millisecond)
			fmt.Println("Waited")
			fmt.Println("Waiting to see if there are subscribers")
			time.Sleep(time.Duration(10) * time.Millisecond)
			fmt.Println("Waited")
			if len(blopSubscribers[write.key]) > 0 {
				fmt.Println("blop subscriber found")
				sub := blopSubscribers[write.key][0]
				blopSubscribers[write.key] = blopSubscribers[write.key][1:]
				sub <- parseStringToRESP(write.val)
				write.ret <- len(lists[write.key]) + 1
			} else {
				if write.left {
					fmt.Println("writing left")
					lists[write.key] = append([]string{write.val}, lists[write.key]...)
				} else {
					fmt.Println("writing right")
					lists[write.key] = append(lists[write.key], write.val)
				}
				write.ret <- len(lists[write.key])
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
