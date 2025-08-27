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
	if score >= current.score { // insert at start
		if score == current.score {
			if member < current.member {
				fmt.Println("Equal score, putting it after first because member smaller")
				newEntry := SortedSetEntry{
					member:  member,
					score:   score,
					smaller: current.smaller,
					greater: current,
					rank:    current.rank,
				}
				current.rank++
				current.smaller = &newEntry
				sortedSets[key][member] = &newEntry
				return
			}
		}
		fmt.Println("Insert ", member, " at start")
		newEntry := SortedSetEntry{
			member:  member,
			score:   score,
			smaller: current,
			greater: nil,
			rank:    current.rank + 1,
		}
		current.greater = &newEntry
		sortedSetsStart[key] = &newEntry
		sortedSets[key][member] = &newEntry
		return
	}

	for current.smaller != nil && current.score > score {
		// moving down the list
		current.rank++
		current = current.smaller
	}
	// insert, check if at the start or after current
	var inserted *SortedSetEntry
	if score == current.score {
		if member < current.member {
			fmt.Println("Equal score, putting it before current because member smaller")
			newEntry := SortedSetEntry{
				member:  member,
				score:   score,
				smaller: current.smaller,
				greater: current,
				rank:    current.rank,
			}
			current.rank++
			current.smaller = &newEntry
			sortedSets[key][member] = &newEntry
			return
		}
		fmt.Println("Equal score, putting it after current because member bigger")
		newEntry := SortedSetEntry{
			member:  member,
			score:   score,
			smaller: current,
			greater: current.greater,
			rank:    current.rank + 1,
		}
		fmt.Println("NewEntry: ", newEntry, "current: ", current)
		current.greater.smaller = &newEntry
		current.greater = &newEntry
		sortedSets[key][member] = &newEntry
		return
	}
	if current.score > score {
		fmt.Println("Smaller rank inserting at the end before current")
		fmt.Println("Current score:", current.score, " member:", current.member, " rank:", current.rank)
		current.smaller = &SortedSetEntry{
			member:  member,
			score:   score,
			greater: current,
			smaller: nil,
			rank:    0,
		}
		inserted = current.smaller
		current.rank = 1
	} else {
		fmt.Println("Insert after current with rank:", current.rank+1)
		fmt.Println("Current score:", current.score, " member:", current.member, " rank:", current.rank)
		current.greater.smaller = &SortedSetEntry{
			member:  member,
			score:   score,
			greater: current.greater,
			smaller: current,
			rank:    current.rank + 1,
		}
		current.greater = current.greater.smaller
		inserted = current.greater
	}
	sortedSets[key][member] = inserted
}

func deleteFromSortedSet(key string, member string) {
	entry := sortedSets[key][member]
	if entry.smaller == nil && entry.greater == nil { // only element
		delete(sortedSets, key)
		delete(sortedSetsStart, key)
		return
	}
	if entry.smaller == nil { // first element
		entry.greater.smaller = nil
		sortedSetsStart[key] = entry.greater
		current := entry.greater
		for current != nil {
			current.rank--
			current = current.greater
		}
		delete(sortedSets[key], member)
		return
	}
	if entry.greater == nil { // last element
		entry.smaller.greater = nil
		sortedSetsStart[key] = entry.smaller
		delete(sortedSets[key], member)
		return
	}
	// middle element
	entry.smaller.greater = entry.greater
	entry.greater.smaller = entry.smaller
	current := entry.greater
	for current != nil {
		current.rank--
		fmt.Println("Lowering ", current.member, "to rank: ", current.rank)
		current = current.greater
	}
	delete(sortedSets[key], member)
}
