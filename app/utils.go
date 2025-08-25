package main

import (
	"net"
	"strconv"
	"time"
)

func addStringToInt(s string, i int) (string, error) {
	number, err := strconv.Atoi(s)
	if err != nil {
		return "", err
	}
	return strconv.Itoa(number + i), nil
}

func searchList(key string, c net.Conn, m bool) (bool, []byte, bool) {
	if val, found := lists[key]; found && listsLock[key][0] == c {
		if len(val) > 0 {
			popped := val[0]
			lists[key] = val[1:]
			listsLock[key] = listsLock[key][1:]
			res := []string{parseStringToRESP(key), parseStringToRESP(popped)}
			return !m, []byte(parseRESPStringsToArray(res)), true
		}
	}
	return !m, []byte(NULLBULK), false
}

func blpopSleep(sleepT int, key string, c net.Conn, m bool, c1 chan<- bool, c2 chan<- []byte) {
	if sleepT > 0 {
		time.Sleep(time.Duration(1) * time.Second)
		m, msg, _ := searchList(key, c, m)
		c1 <- m
		c2 <- msg
	} else {
		for {
			m, msg, found := searchList(key, c, m)
			if found {
				c1 <- m
				c2 <- msg
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
