package main

import (
	"./rfslib"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func get_local_miner_ip_addresses(fname string) (string, string, error) {
	// This assumes that miner file only has the miner ip address:port as the content
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		return "", "", err
	}
	s := string(data)
	s = strings.TrimSuffix(s, "\n")
	ips := strings.Split(s, "\n")
	return ips[0], ips[1], nil
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: go run touch.go <fname>")
	}

	fname := os.Args[1]
	local_ip, miner_address, err := get_local_miner_ip_addresses("./.rfs")
	if err != nil {
		log.Fatal("Failed to obtain ip addresses from ./.rfs")
	}

	rfs, err := rfslib.Initialize(local_ip, miner_address)
	if err != nil {
		log.Fatal("Failed to initialize rfslib")
	}

	err = rfs.CreateFile(fname)
	if err != nil {
		log.Fatal("Failed to create file: ", fname)
	}
	fmt.Println("Successfully created file:", fname)
}
