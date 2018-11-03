package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.ugrad.cs.ubc.ca/CPSC416-2018W-T1/P1-i8b0b-e8y0b/rfslib"
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
	if len(os.Args) != 3 {
		log.Fatal("Usage: go run append.go <fname> <record_string>")
	}
	fname := os.Args[1]
	record_string := os.Args[2]
	local_ip, miner_address, err := get_local_miner_ip_addresses("./.rfs")
	log.Println("miner_address", miner_address)
	if err != nil {
		log.Fatal("Failed to obtain ip addresses from ./.rfs")
	}

	rfs, err := rfslib.Initialize(local_ip, miner_address)
	if err != nil {
		log.Fatal("Failed to initialize rfslib")
	}

	var record rfslib.Record
	copy(record[:], record_string)
	record_num, err := rfs.AppendRec(fname, &record)
	if err != nil {
		log.Fatalf("Failed to append %s to file %f\n", record_string, fname)
	}
	fmt.Printf("Succesfully appended a record to file %s at index %d", fname, record_num)
}
