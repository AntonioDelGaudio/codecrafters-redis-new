package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"net"
	"strconv"
	"strings"
)

func parseCommand(commands []string, c net.Conn, master bool, bCount int) (respond bool, out []byte) {
	switch commands[0][0] {
	case '*':
		return handleArray(commands, c, master, bCount)
	case '$':
		return handleString(commands, master, bCount)
	case '%':
		return handleMap(commands, master, bCount)
	}
	return true, []byte("$22\r\nCommand not recognized\r\n")
}

func parseStringToRESP(s string) string {
	return "$" + strconv.Itoa(len(s)) + CRLF + s + CRLF
}

func parseMapToRESPBulkString(m map[string]string) []byte {
	res := ""
	for k, v := range m {
		cString := k + ":" + v
		res += cString + CRLF
	}
	return []byte("$" + strconv.Itoa(len(res)-2) + CRLF + res)
}

func parseRESPStringsToArray(s []string) string {
	return "*" + strconv.Itoa(len(s)) + CRLF + strings.Join(s, "")
}

func joinWithCRLF(txt []string) string {
	return strings.Join(txt, CRLF) + CRLF
}

func stripCRLF(txt []byte) []byte {
	txt = bytes.ReplaceAll(txt, []byte{CR}, []byte{})
	txt = bytes.ReplaceAll(txt, []byte{LF}, []byte{})
	return txt
}

func parseFromRESP(reader *bufio.Reader) (res []string, count int) {
	var start []byte
	var bCount int
	for len(start) == 0 {
		start, _ = reader.ReadBytes(LF)
		bCount += len(start)
		start = stripCRLF(start)
	}
	if string(start[0]) == "*" {
		res = append(res, string(start))
		count, _ := strconv.Atoi(string(start[1:]))
		for i := 0; i < count*2; {
			txt, _ := reader.ReadBytes(LF)
			bCount += len(txt)
			txt = stripCRLF(txt)
			res = append(res, string(txt))
			i++
		}
	}
	return res, bCount
}

func parseBase64(s string) []byte {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func parseStringToRESPInt(i string) string {
	return ":" + i + CRLF
}
