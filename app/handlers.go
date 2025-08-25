package main

import (
	"bufio"
	"net"
	"strings"
)

func handleMessage(c net.Conn, master bool, reader *bufio.Reader) {
	for {
		cmds, bCount := parseFromRESP(reader)
		if replicationConfigs["role"] == "master" && isWrite[strings.ToLower(cmds[2])] {
			sent = true
			alignedRepl.Reset()
			for _, slave := range replConnections {
				msg := joinWithCRLF(cmds)
				go func() {
					toConnection([]byte(msg), slave)
				}()
			}
		}
		needResponse, output := parseCommand(cmds, c, master, bCount)
		if needResponse {
			toConnection(output, c)
		}
	}

}

func handleArray(cmds []string, c net.Conn, master bool, bCount int) (respond bool, out []byte) {
	command := strings.ToLower(cmds[2])
	if _, ok := extraCommands[command]; ok {
		return extraCommands[command](cmds, c, master, bCount)
	}
	if _, ok := commands[command]; !ok { // if not return error
		return !master, []byte("-ERR unknown command '" + command + "'" + CRLF)
	}
	if inTrans[c] {
		queue[c] = append(queue[c], cmds)
		return true, []byte("+QUEUED" + CRLF)
	}
	return commands[command](cmds, c, master, bCount)
}

func handleString(_ []string, _ bool, _ int) (bool, []byte) {
	return false, []byte("")
}

func handleMap(_ []string, _ bool, _ int) (bool, []byte) {
	return false, []byte("")
}

func handleOffset(bCount int) {
	replicationConfigs["master_repl_offset"], _ = addStringToInt(replicationConfigs["master_repl_offset"], bCount)
}
