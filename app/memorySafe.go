package main

import (
	"sync"
)

type SafeCounter struct {
	mu sync.Mutex
	v  int
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
