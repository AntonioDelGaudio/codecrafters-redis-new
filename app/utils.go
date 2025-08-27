package main

import (
	"fmt"
	"strconv"
)

func addStringToInt(s string, i int) (string, error) {
	number, err := strconv.Atoi(s)
	if err != nil {
		return "", err
	}
	return strconv.Itoa(number + i), nil
}

func addToSortedSet(key string, member string, score float64) {
	current := sortedSetsStart[key]
	for current.prev != nil && current.score > score {
		fmt.Println("From prev score:", current.score, " member:", current.member, " rank:", current.rank)
		current.rank++
		current = current.prev
	}
	// insert, check if at the start or after current
	var inserted *SortedSetEntry
	if current.prev == nil {
		fmt.Println("Insert at start")
		current.prev = &SortedSetEntry{
			member: member,
			score:  score,
			next:   current,
			prev:   nil,
			rank:   0,
		}
		sortedSetsStart[key] = current.prev
		inserted = current.prev
	} else {
		fmt.Println("Insert after current with rank:", current.rank+1)
		current.next.prev = &SortedSetEntry{
			member: member,
			score:  score,
			next:   current.next,
			prev:   current,
			rank:   current.rank + 1,
		}
		current.next = current.next.prev
		inserted = current.next
	}
	sortedSets[key][member] = inserted
}

func deleteFromSortedSet(key string, member string) {
	entry := sortedSets[key][member]
	if entry.prev == nil && entry.next == nil { // only element
		delete(sortedSets, key)
		delete(sortedSetsStart, key)
		return
	}
	if entry.prev == nil { // first element
		entry.next.prev = nil
		sortedSetsStart[key] = entry.next
		current := entry.next
		for current != nil {
			current.rank--
			current = current.next
		}
		delete(sortedSets[key], member)
		return
	}
	if entry.next == nil { // last element
		entry.prev.next = nil
		delete(sortedSets[key], member)
		return
	}
	// middle element
	entry.prev.next = entry.next
	entry.next.prev = entry.prev
	current := entry.next
	for current != nil {
		current.rank--
		current = current.next
	}
	delete(sortedSets[key], member)
}
