package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

var queue = map[net.Conn][][]string{}
var inTrans = map[net.Conn]bool{}

var commands = map[string]func(splittedCommand []string, c net.Conn, master bool, bCount int) (bool, []byte){
	"get":      get,
	"echo":     echo,
	"set":      set,
	"ping":     ping,
	"info":     info,
	"replconf": replconf,
	"psync":    psync,
	"wait":     wait,
	"config":   config,
	"keys":     keys,
	"incr":     incr,
	"multi":    multi,
	"type":     typeC,
	"xadd":     xadd,
	"xrange":   xrange,
	"xread":    xread,
	"rpush":    rpush,
	"lrange":   lrange,
	"lpush":    lpush,
	"llen":     llen,
	"lpop":     lpop,
	"blpop":    blpop,
}

var extraCommands = map[string]func(splittedCommand []string, c net.Conn, master bool, bCount int) (bool, []byte){
	"exec":    exec,
	"discard": discard,
}

var isWrite = map[string]bool{
	"set": true,
}

func psync(_ []string, c net.Conn, _ bool, bCount int) (bool, []byte) {
	parsedRDB := parseBase64(EMPTY_RDB_64)
	toConnection([]byte("+FULLRESYNC "+replicationConfigs["master_replid"]+" 0"+CRLF), c)
	toConnection(append([]byte("$"+strconv.Itoa(len(parsedRDB))+CRLF), parsedRDB...), c)
	replConnections = append(replConnections, c)
	replicationConfigs["connected_slaves"] = strconv.Itoa(len(replConnections))
	handleOffset(bCount)
	return false, nil
}

func replconf(cmds []string, _ net.Conn, _ bool, bCount int) (bool, []byte) {
	switch strings.ToLower(cmds[4]) {
	case "getack":
		offset := replicationConfigs["master_repl_offset"]
		handleOffset(bCount)
		return true, []byte(parseRESPStringsToArray([]string{
			parseStringToRESP("REPLCONF"),
			parseStringToRESP("ACK"),
			parseStringToRESP(offset),
		}))
	case "ack":
		alignedRepl.Inc()
	default:
		handleOffset(bCount)
		return true, []byte(OK)
	}
	return false, nil
}

func info(commands []string, _ net.Conn, master bool, bCount int) (bool, []byte) {
	for k, v := range replicationConfigs {
		configs[k] = v
	}
	if commands[0] != "*3" {
		switch strings.ToLower(commands[2]) {
		case "replication":
			return !master, parseMapToRESPBulkString(replicationConfigs)
		default:
			return !master, parseMapToRESPBulkString(configs)
		}
	}
	handleOffset(bCount)
	return !master, parseMapToRESPBulkString(configs)
}

func echo(commands []string, _ net.Conn, master bool, bCount int) (bool, []byte) {
	handleOffset(bCount)
	return !master, []byte(parseStringToRESP(commands[4]))
}

func ping(_ []string, _ net.Conn, master bool, bCount int) (bool, []byte) {
	handleOffset(bCount)
	return !master, []byte("+PONG" + CRLF)
}

func set(commands []string, _ net.Conn, master bool, bCount int) (bool, []byte) {
	newRecord := Record{
		value:    commands[6],
		expiry:   false,
		ttl:      0,
		modified: time.Now(),
	}
	if commands[0] != "*3" && strings.ToLower(commands[8]) == "px" {
		newRecord.expiry = true
		ttl, err := strconv.Atoi(commands[10])
		if err != nil {
			return false, []byte("-ERROR: Invalid TTL" + CRLF)
		}
		newRecord.ttl = time.Duration(ttl) * time.Millisecond
	}
	memory[commands[4]] = newRecord
	handleOffset(bCount)
	return !master, []byte("+OK\r\n")
}

func get(splittedCommand []string, _ net.Conn, master bool, bCount int) (bool, []byte) {
	if val, found := memory[splittedCommand[4]]; found {
		if val.expiry && time.Now().After(val.modified.Add(val.ttl)) {
			return !master, []byte(NULLBULK)
		}
		return !master, []byte(parseStringToRESP(val.value))
	}
	handleOffset(bCount)
	return !master, []byte(NULLBULK)
}

func wait(splittedCommand []string, _ net.Conn, master bool, bCount int) (bool, []byte) {
	if !sent {
		handleOffset(bCount)
		return !master, []byte(parseStringToRESPInt(replicationConfigs["connected_slaves"]))
	}
	ttl, _ := strconv.Atoi(splittedCommand[6])
	replNeeded, _ := strconv.Atoi(splittedCommand[4])
	c := make(chan bool)
	for _, slave := range replConnections {
		toConnection([]byte(parseRESPStringsToArray([]string{
			parseStringToRESP("REPLCONF"),
			parseStringToRESP("GETACK"),
			parseStringToRESP("*"),
		})), slave)
	}
	go func() {
		for {
			if alignedRepl.IsEnough(replNeeded) {
				c <- true
				return
			}
			time.Sleep(time.Duration(1) * time.Millisecond)
		}
	}()
	select {
	case <-c:
		handleOffset(bCount)
		return !master, []byte(parseStringToRESPInt(strconv.Itoa(replNeeded)))
	case <-time.After(time.Duration(ttl) * time.Millisecond):
		handleOffset(bCount)
		return !master, []byte(parseStringToRESPInt(strconv.Itoa(alignedRepl.Value())))
	}
}

func config(cmds []string, _ net.Conn, master bool, bCount int) (bool, []byte) {
	switch cmds[4] {
	case "GET":
		return !master, []byte(parseRESPStringsToArray([]string{
			parseStringToRESP(cmds[6]),
			parseStringToRESP(configs[cmds[6]])}))
	}

	return !master, []byte("DAFUQ")
}

func keys(cmds []string, _ net.Conn, master bool, bCount int) (bool, []byte) {
	if cmds[4] == "*" {
		var res []string
		for k := range memory {
			res = append(res, parseStringToRESP(k))
		}
		return !master, []byte(parseRESPStringsToArray(res))
	}
	return !master, []byte("NOT YET")
}

func incr(cmds []string, _ net.Conn, m bool, count int) (bool, []byte) {
	key := cmds[4]
	if _, ok := memory[key]; !ok { // if not available set it
		memory[key] = Record{
			value:    "0",
			expiry:   false,
			ttl:      0,
			modified: time.Now(),
		}
	} else if _, err := strconv.Atoi(memory[key].value); err != nil {
		return !m, []byte("-ERR value is not an integer or out of range" + CRLF)
	}
	handleOffset(count)
	i, _ := strconv.Atoi(memory[key].value)
	memory[key] = Record{
		value:    strconv.Itoa(i + 1),
		expiry:   false,
		ttl:      0,
		modified: time.Now(),
	}
	return !m, []byte(parseStringToRESPInt(memory[key].value))
}

func multi(cmds []string, c net.Conn, m bool, count int) (bool, []byte) {
	inTrans[c] = true
	return !m, []byte("+OK" + CRLF)
}

func exec(_ []string, c net.Conn, m bool, count int) (bool, []byte) {
	if !inTrans[c] {
		return !m, []byte("-ERR EXEC without MULTI" + CRLF)
	}
	var queuedResp []string
	for _, cmds := range queue[c] {
		command := strings.ToLower(cmds[2])
		needResponse, output := commands[command](cmds, c, m, count)
		if needResponse {
			queuedResp = append(queuedResp, string(output))
		}
	}
	delete(inTrans, c)
	delete(queue, c)
	return !m, []byte(parseRESPStringsToArray(queuedResp))
}

func discard(cmds []string, c net.Conn, m bool, count int) (bool, []byte) {
	if !inTrans[c] {
		return !m, []byte("-ERR DISCARD without MULTI" + CRLF)
	}
	delete(inTrans, c)
	delete(queue, c)
	return !m, []byte("+OK" + CRLF)
}

func typeC(cmds []string, c net.Conn, m bool, count int) (bool, []byte) {
	if _, ok := memory[cmds[4]]; ok {
		return !m, []byte("+string" + CRLF)
	} else if _, ok := entries[cmds[4]]; ok {
		return !m, []byte("+stream" + CRLF)
	} else {
		return !m, []byte("+none" + CRLF)
	}
}

// xCommands implementation
func xadd(cmds []string, c net.Conn, m bool, count int) (bool, []byte) {
	var lastId string
	if v, ok := entries[cmds[4]]; ok {
		lastId = v[len(v)-1].id
	} else {
		entries[cmds[4]] = []Entry{}
	}

	id := cmds[6]
	id = handleId(lastId, id)
	if id <= "0-0" {
		return !m, []byte("-ERR The ID specified in XADD must be greater than 0-0" + CRLF)
	}
	if len(entries[cmds[4]]) > 0 && !isIdGTLast(lastId, id) {
		return !m, []byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item" + CRLF)
	}

	newEntry := Entry{
		id: id,
		values: map[string]string{
			cmds[8]: cmds[10],
		},
	}

	entries[cmds[4]] = append(entries[cmds[4]], newEntry)
	return !m, []byte(parseStringToRESP(id))
}

func xrange(cmds []string, c net.Conn, m bool, count int) (bool, []byte) {
	key := cmds[4]
	startSplitted := strings.Split(cmds[6], "-")
	endSplitted := strings.Split(cmds[8], "-")
	if len(startSplitted) == 1 {
		startSplitted = append(startSplitted, "0")
	}
	if len(endSplitted) == 1 {
		endSplitted = append(endSplitted, "999999")
	}
	res := filterEntries(entries[key], startSplitted, endSplitted, isInRange)
	return !m, []byte(parseRESPStringsToArray(res))
}

func xread(cmds []string, c net.Conn, m bool, bCount int) (bool, []byte) {
	count, _ := strconv.Atoi(strings.Replace(cmds[0], "*", "", 1))
	nStreams := (count - 2) / 2 // The number of streams required is the number of args once removed "xread" and "streams" and divided by 2 (key and id)
	var j int
	var found bool
	if strings.ToLower(cmds[4]) == "block" {
		if cmds[len(cmds)-1] == "$" {
			key := cmds[10]
			lastId := entries[key][len(entries[key])-1].id
			cmds[len(cmds)-1] = lastId
		}
		nStreams--
		j = 4 // displace if block command
		sleepT, _ := strconv.Atoi(cmds[6])
		if sleepT > 0 {
			time.Sleep(time.Duration(sleepT) * time.Millisecond)
		} else {
			for {
				found, externalSlice := checkStreams(nStreams, cmds, j)
				if found {
					return !m, []byte(parseRESPStringsToArray(externalSlice))
				}
				time.Sleep(time.Duration(1000) * time.Millisecond)
			}
		}
	}
	found, externalSlice := checkStreams(nStreams, cmds, j)
	if found {
		return !m, []byte(parseRESPStringsToArray(externalSlice))
	}
	return !m, []byte(NULLBULK)
}

// lists implementation
func rpush(cmds []string, c net.Conn, m bool, bCount int) (bool, []byte) {
	var newVals []string
	for i := 6; i < len(cmds); i += 2 {
		newVals = append(newVals, cmds[i])
	}
	if val, found := lists[cmds[4]]; found {
		lists[cmds[4]] = append(val, newVals...)
	} else {
		lists[cmds[4]] = newVals
	}
	return !m, []byte(parseStringToRESPInt(strconv.Itoa(len(lists[cmds[4]]))))
}

func lrange(cmds []string, c net.Conn, m bool, count int) (bool, []byte) {
	start, _ := strconv.Atoi(cmds[6])
	end, _ := strconv.Atoi(cmds[8])
	if val, found := lists[cmds[4]]; found {
		if start < 0 {
			start = len(val) + start
		}
		if end < 0 {
			end = len(val) + end
		}
		if start < 0 {
			start = 0
		}
		if end < 0 {
			end = 0
		}
		if end >= len(val) {
			end = len(val) - 1
		}
		if start > end || start >= len(val) {
			return !m, []byte(parseRESPStringsToArray([]string{}))
		}
		var res []string
		for i := start; i <= end; i++ {
			res = append(res, parseStringToRESP(val[i]))
		}
		return !m, []byte(parseRESPStringsToArray(res))
	}
	return !m, []byte(parseRESPStringsToArray([]string{}))
}

func lpush(cmds []string, c net.Conn, m bool, bCount int) (bool, []byte) {
	var newVals []string
	for i := 6; i < len(cmds); i += 2 {
		newVals = append([]string{cmds[i]}, newVals...)
	}
	if val, found := lists[cmds[4]]; found {
		lists[cmds[4]] = append(newVals, val...)
	} else {
		lists[cmds[4]] = newVals
	}
	return !m, []byte(parseStringToRESPInt(strconv.Itoa(len(lists[cmds[4]]))))
}

func llen(cmds []string, c net.Conn, m bool, bCount int) (bool, []byte) {
	if val, found := lists[cmds[4]]; found {
		return !m, []byte(parseStringToRESPInt(strconv.Itoa(len(val))))
	}
	return !m, []byte(parseStringToRESPInt("0"))
}

func lpop(cmds []string, c net.Conn, m bool, bCount int) (bool, []byte) {
	if val, found := lists[cmds[4]]; found {
		if len(val) > 0 {
			if len(cmds) > 5 {
				nPop, _ := strconv.Atoi(cmds[6])
				var res []string
				for i := 0; i < nPop; i++ {
					popped := lists[cmds[4]][0]
					res = append(res, parseStringToRESP(popped))
					lists[cmds[4]] = lists[cmds[4]][1:]
				}
				return !m, []byte(parseRESPStringsToArray(res))
			}
			popped := val[0]
			lists[cmds[4]] = val[1:]
			return !m, []byte(parseStringToRESP(popped))
		}
	}
	return !m, []byte(NULLBULK)
}

func blpop(cmds []string, c net.Conn, m bool, bCount int) (bool, []byte) {
	if val, found := lists[cmds[4]]; found {
		if len(val) > 0 {
			popped := val[0]
			lists[cmds[4]] = val[1:]
			return !m, []byte(parseStringToRESP(popped))
		}
	}
	sleepT, _ := strconv.Atoi(cmds[6])
	fmt.Println(sleepT)
	listsLock[cmds[4]] = append(listsLock[cmds[4]], c)
	if sleepT > 0 {
		time.Sleep(time.Duration(sleepT) * time.Millisecond)
	} else {
		for {
			if val, found := lists[cmds[4]]; found && listsLock[cmds[4]][0] == c {
				if len(val) > 0 {
					popped := val[0]
					lists[cmds[4]] = val[1:]
					listsLock[cmds[4]] = listsLock[cmds[4]][1:]
					return !m, []byte(parseStringToRESP(popped))
				}
			}
			time.Sleep(time.Duration(10) * time.Millisecond)
		}
	}
	return !m, []byte(NULLBULK)
}

func checkStreams(nStreams int, cmds []string, j int) (bool, []string) {
	var found bool
	var externalSlice []string
	for i := 0; i < nStreams; i++ {
		key := cmds[6+2*i+j]
		startSplitted := strings.Split(cmds[6+2*nStreams+2*i+j], "-")
		res := []string{
			parseStringToRESP(key),
		}
		innerRes := filterEntries(entries[key], startSplitted, nil, isAfter)
		if len(innerRes) > 0 {
			found = true
		}
		res = append(res, parseRESPStringsToArray(innerRes))
		externalSlice = append(externalSlice, parseRESPStringsToArray(res))
	}
	return found, externalSlice
}
