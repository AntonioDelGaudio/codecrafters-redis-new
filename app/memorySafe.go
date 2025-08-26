package main

import (
	"fmt"
	"net"
	"sync"
)

type SafeCounter struct {
	mu sync.Mutex
	v  int
}

type Subscriber struct {
	conn net.Conn
	// A unique ID for this specific BLPOP request. This is crucial to avoid
	// cancelling the wrong request if a client disconnects and reconnects.
	id uint64
}
type ReadReq struct {
	block bool // True for BLPOP, false for LPOP.
	key   string
	conn  net.Conn    // For BLPOP, the connection to write the response to.
	id    uint64      // The unique ID for a BLPOP request.
	ret   chan string // For LPOP, the channel for an immediate response.
}

type WriteReq struct {
	key  string
	left bool
	val  string
	ret  chan int
}

type LenReq struct {
	key string
	ret chan int
}

// A request to get a range of elements from a list.
type RangeReq struct {
	key   string
	start int
	end   int
	ret   chan []string
}

type CancelReq struct {
	key string
	id  uint64 // The ID of the request to cancel.
}

var nextSubscriberID uint64

var (
	writeChan  = make(chan WriteReq)
	readChan   = make(chan ReadReq)
	cancelChan = make(chan CancelReq)
	lenChan    = make(chan LenReq)
	rangeChan  = make(chan RangeReq)
)

var (
	lists           = make(map[string][]string)
	blopSubscribers = make(map[string][]*Subscriber)
)

func listBroker(writeChan <-chan WriteReq, readChan <-chan ReadReq, cancelChan <-chan CancelReq, lenChan <-chan LenReq, rangeChan <-chan RangeReq) {
	for {
		select {
		case write := <-writeChan:
			if len(blopSubscribers[write.key]) > 0 {
				sub := blopSubscribers[write.key][0]
				blopSubscribers[write.key] = blopSubscribers[write.key][1:]
				response := parseRESPStringsToArray([]string{parseStringToRESP(write.key), parseStringToRESP(write.val)})
				if _, err := sub.conn.Write([]byte(response)); err != nil {
					fmt.Printf("Error writing to subscriber connection: %v\n", err)
				}
				fmt.Printf("Served subscriber %d for key %s with value %s\n", sub.id, write.key, write.val)
				write.ret <- len(lists[write.key]) + 1
			} else {
				if write.left {
					lists[write.key] = append([]string{write.val}, lists[write.key]...)
				} else {
					lists[write.key] = append(lists[write.key], write.val)
				}
				write.ret <- len(lists[write.key])
			}

		case read := <-readChan:
			if read.block {
				if len(lists[read.key]) > 0 {
					popped := lists[read.key][0]
					lists[read.key] = lists[read.key][1:]
					response := parseRESPStringsToArray([]string{parseStringToRESP(read.key), parseStringToRESP(popped)})
					read.conn.Write([]byte(response))
				} else {
					fmt.Printf("Adding subscriber %d for key %s\n", read.id, read.key)
					newSub := &Subscriber{conn: read.conn, id: read.id}
					blopSubscribers[read.key] = append(blopSubscribers[read.key], newSub)
				}
			} else { // Non-blocking LPOP
				if len(lists[read.key]) > 0 {
					popped := lists[read.key][0]
					lists[read.key] = lists[read.key][1:]
					read.ret <- parseStringToRESP(popped)
				} else {
					read.ret <- NULLBULK
				}
			}

		case cancel := <-cancelChan:
			subs := blopSubscribers[cancel.key]
			for i, sub := range subs {
				if sub.id == cancel.id {
					fmt.Printf("Cancelling request %d for key %s\n", cancel.id, cancel.key)
					sub.conn.Write([]byte(NULLBULK))
					subs[i] = subs[len(subs)-1]
					blopSubscribers[cancel.key] = subs[:len(subs)-1]
					break
				}
			}

		case req := <-lenChan:
			req.ret <- len(lists[req.key])

		case req := <-rangeChan:
			val := lists[req.key]
			start, end := req.start, req.end
			if start < 0 {
				start = len(val) + start
			}
			if end < 0 {
				end = len(val) + end
			}
			if start < 0 {
				start = 0
			}
			if end >= len(val) {
				end = len(val) - 1
			}
			if start > end || start >= len(val) {
				req.ret <- []string{}
			} else {
				var res []string
				for i := start; i <= end; i++ {
					res = append(res, parseStringToRESP(val[i]))
				}
				req.ret <- res
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
