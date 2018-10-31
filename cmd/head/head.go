package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
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
		log.Fatal("Usage: go run head.go <k> <fname>")
	}
	k, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal("Failed to convert k to a number.", err)
	}
	fname := os.Args[2]
	local_ip, miner_address, err := get_local_miner_ip_addresses("./.rfs")
	if err != nil {
		log.Fatal("Failed to obtain ip addresses from ./.rfs")
	}

	rfs, err := rfslib.Initialize(local_ip, miner_address)
	if err != nil {
		log.Fatal("Failed to initialize rfslib")
	}

	num_recs, err := rfs.TotalRecs(fname)
	if err != nil {
		log.Fatal("Failed to obtain total number of records for: ", fname)
	}

	var i uint16

	for i = 0; i < num_recs; i++ {
		if i < uint16(k) {
			var record rfslib.Record
			err := rfs.ReadRec(fname, i, &record)
			if err != nil {
				log.Fatalf("Failed to obtain record %d for %s\n", i, fname)
			}
			fmt.Println(string(record[:]))
		}
	}
}
