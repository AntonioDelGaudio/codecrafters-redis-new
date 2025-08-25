package main

import (
	"strconv"
	"strings"
	"time"
)

func isIdGTLast(lastId string, newId string) bool {
	// check if last id is greater than the proposed one

	lastSplitted := strings.SplitAfter(lastId, "-")
	newSplitted := strings.SplitAfter(newId, "-")
	return lastSplitted[0] < newSplitted[0] ||
		lastSplitted[0] == newSplitted[0] && lastSplitted[1] < newSplitted[1]
}

func handleId(lastId string, id string) string {
	idSplit := strings.Split(id, "-")
	if idSplit[0] == "*" {
		return strconv.Itoa(int(time.Now().UnixMilli())) + "-0"
	}
	if idSplit[1] == "*" {
		if idSplit[0] == "0" && lastId == "" {
			return "0-1"
		}
		if lastId == "" {
			return idSplit[0] + "-" + "0"
		}
		lastSplitted := strings.Split(lastId, "-")
		if lastSplitted[0] == idSplit[0] {
			newVal, _ := strconv.Atoi(lastSplitted[1])
			newVal++
			return idSplit[0] + "-" + strconv.Itoa(newVal)
		}
		return idSplit[0] + "-" + "0"
	}
	return id
}

func isInRange(idSplitted []string, startSplitted []string, endSplitted []string) bool {
	return (len(startSplitted) == 0 || // The start is "-"
		(idSplitted[0] > startSplitted[0] || // the timestamp part of the id is greater than the start interval
			idSplitted[0] == startSplitted[0] && idSplitted[1] >= startSplitted[1])) && // the timestamp part of the id is the same but the progressive is greater or equal than the progressive part of the start interval
		(endSplitted[0] == "+" || // the end is "+"
			(idSplitted[0] < endSplitted[0] || // the timestamp part of the id is smaller than the end interval
				idSplitted[0] == endSplitted[0] && idSplitted[1] <= endSplitted[1])) // the timestamp part of the id is the same but the progressive is smaller or equal than the progressive part of the end interval
}

func isAfter(idSplitted []string, startSplitted []string, _ []string) bool {
	return idSplitted[0] > startSplitted[0] || idSplitted[0] == startSplitted[0] && idSplitted[1] > startSplitted[1]
}

func filterEntries(entries []Entry, startSplitted []string, endSplitted []string, filterFunc func([]string, []string, []string) bool) []string {
	var res []string
	for _, entry := range entries {
		var middleRes []string
		idSplitted := strings.Split(entry.id, "-")
		if filterFunc(idSplitted, startSplitted, endSplitted) {
			middleRes = append(middleRes, parseStringToRESP(entry.id))
			var internalRes []string
			for k, v := range entry.values {
				internalRes = append(internalRes, parseStringToRESP(k))
				internalRes = append(internalRes, parseStringToRESP(v))
			}
			middleRes = append(middleRes, parseRESPStringsToArray(internalRes))
			res = append(res, parseRESPStringsToArray(middleRes))
		}
	}
	return res
}
