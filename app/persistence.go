package main

import (
	"bufio"
	"fmt"
	"os"
)

const magicString = "REDIS0011"

func restore() {
	if configs["dir"] != "" && configs["dbfilename"] != "" {
		f, err := os.Open(configs["dir"] + "/" + configs["dbfilename"])
		defer f.Close()
		if err != nil {
			fmt.Println("Cannot open RDB file, ignoring it")
			return
		}
		r := bufio.NewReader(f)
		data := parseDb(r)
		for k, v := range data {
			memory[k] = v
		}
	}
}

func snapshot() {

}
