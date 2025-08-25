package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Record struct {
	value    string
	expiry   bool
	ttl      time.Duration
	modified time.Time
}

type Entry struct {
	id     string
	values map[string]string
}

var port = flag.String("port", "6379", "The port to listen on")
var replicaof = flag.String("replicaof", "", "The replica of this server")
var dir = flag.String("dir", "", "The directory in which the data will be saved for recovery")
var dbfilename = flag.String("dbfilename", "", "The name of the file with the snapshots")

var entries = make(map[string][]Entry)
var memory = make(map[string]Record)
var lists = make(map[string][]string)
var sent = false

var alignedRepl = SafeCounter{
	mu: sync.Mutex{},
	v:  0,
}

var configs = map[string]string{
	"port": "6379",
}

var replConnections []net.Conn

var replicationConfigs = map[string]string{
	"role":                           "master",
	"connected_slaves":               "0",
	"master_replid":                  strings.ReplaceAll(uuid.New().String(), "-", ""),
	"master_repl_offset":             "0",
	"second_repl_offset":             "-1",
	"repl_backlog_active":            "0",
	"repl_backlog_size":              "1048576",
	"repl_backlog_first_byte_offset": "0",
	"repl_backlog_histlen":           "",
}

var master net.Conn

func toMaster(msg []byte) {
	_, err := master.Write(msg)
	if err != nil {
		fmt.Println("Error writing to master", err.Error())
		os.Exit(1)
	}
}
func receiveRDB(buf []byte) {
	fromMaster(buf)
	bts := bytes.Split(buf, []byte(CRLF))
	fmt.Print(string(bts[0]))
}

func fromMaster(buf []byte) {
	_, err := master.Read(buf)
	if err != nil {
		fmt.Println("Error reading from master", err.Error())
		os.Exit(1)
	}
}

func toConnection(buf []byte, conn net.Conn) {
	_, err := conn.Write(buf)
	if err != nil {
		fmt.Println("Error writing to connection", err.Error())
		os.Exit(1)
	}
}

func connectToMaster(addr string) {
	var err error
	master, err = net.Dial("tcp", addr)
	reader := bufio.NewReader(master)
	if err != nil {
		fmt.Println("Failed to connect to master on" + *replicaof)
		os.Exit(1)
	}
	toMaster([]byte(PING))
	_, _ = reader.ReadString(CR)
	toMaster([]byte(parseRESPStringsToArray([]string{
		parseStringToRESP("REPLCONF"),
		parseStringToRESP(LISTENING_PORTS),
		parseStringToRESP(*port),
	})))
	_, _ = reader.ReadString(CR)
	toMaster([]byte(parseRESPStringsToArray([]string{
		parseStringToRESP("REPLCONF"),
		parseStringToRESP("capa"),
		parseStringToRESP("psync2"),
	})))
	_, _ = reader.ReadString(CR)
	toMaster([]byte(parseRESPStringsToArray([]string{
		parseStringToRESP("PSYNC"),
		parseStringToRESP("?"),
		parseStringToRESP("-1"),
	})))
	_, _ = reader.ReadString(CR)
	lns, _ := reader.ReadString(CR)
	lns = strings.TrimSuffix(lns, "\r")
	lns = strings.TrimPrefix(lns, "\n")
	ln, err := strconv.Atoi(strings.Replace(lns, "$", "", -1))
	reader.Discard(ln + 1)
	replicationConfigs["role"] = "slave"
	go handleMessage(master, true, reader)
}

func main() {
	flag.Parse()
	if *replicaof != "" && *replicaof != "master" {
		connectToMaster(strings.Replace(*replicaof, " ", ":", 1))
	}
	configs["port"] = *port
	configs["dir"] = *dir
	configs["dbfilename"] = *dbfilename
	restore()
	l, err := net.Listen("tcp", "0.0.0.0:"+*port)
	if err != nil {
		fmt.Println("Failed to bind to port " + *port)
		os.Exit(1)
	}
	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		defer c.Close()
		reader := bufio.NewReader(c)
		go handleMessage(c, false, reader)
	}
}
