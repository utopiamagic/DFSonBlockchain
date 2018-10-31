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
	if len(os.Args) != 1 && len(os.Args) != 2 {
		log.Fatal("Usage: go run ls.go [-a]")
	}
	list_num_records := false
	if len(os.Args) == 2 {
		if os.Args[1] != "-a" {
			log.Fatal("Usage: go run ls.go [-a]")
		} else {
			list_num_records = true
		}
	}

	local_ip, miner_address, err := get_local_miner_ip_addresses("./.rfs")
	if err != nil {
		log.Fatal("Failed to obtain ip addresses from ./.rfs")
	}

	rfs, err := rfslib.Initialize(local_ip, miner_address)
	if err != nil {
		log.Fatal("Failed to initialize rfslib")
	}

	flist, err := rfs.ListFiles()
	if err != nil {
		log.Fatal("Failed to obtain list of files")
	}

	for _, fname := range flist {
		if list_num_records {
			num_recs, err := rfs.TotalRecs(fname)
			if err != nil {
				log.Fatal("Failed to obtain total number of records for: ", fname)
			}
			fmt.Println(fname, num_recs)
		} else {
			fmt.Println(fname)
		}
	}
}
