/*
Implements the solution to assignment 1 for UBC CS 416 2017 W2.

Usage:
$ go run client.go [local UDP ip:port] [local TCP ip:port] [aserver UDP ip:port]

Example:
$ go run client.go 127.0.0.1:2020 127.0.0.1:3030 127.0.0.1:7070

*/

package main

import (
	"crypto/md5"
	"encoding/hex"

	"github.ugrad.cs.ubc.ca/CPSC416-2018W-T1/P1-i8b0b-e8y0b/rfslib"
	// TODO
)

// OPBlock: An op block is a data structure that contains at least the following data
type OPBlock struct {
	PrevHash string            // A hash of the previous block in the chain (prev-hash)
	Records  []OperationRecord // An ordered set of operation records
	MinerID  string            // The identifier of the miner that computed this block (block-minerID)
	Nonce    uint32            //A 32-bit unsigned integer nonce (nonce)
}

type OperationRecord struct {
	RecordData rfslib.Record // rfslib operation details (op)
	RecordNum  uint16
	MinerID    string // An identifier that specifies the miner identifier whose record coins sponsor this operation (op-minerID)
}

// Miner is blah
type Miner struct {
	MinerID             string   // The ID of this miner (max 16 characters).
	PeerMinersAddrs     []string // An array of remote IP:port addresses, one per peer miner that this miner should connect to (using the OutgoingMinersIP below)
	IncomingMinersAddr  string   // The local IP:port where the miner should expect other miners to connect to it (address it should listen on for connections from miners)
	OutgoingMinersIP    string   // The local IP that the miner should use to connect to peer miners
	IncomingClientsAddr string   // The local IP:port where this miner should expect to receive connections from RFS clients (address it should listen on for connections from clients)
}

/////////// Msgs used by both auth and fortune servers:

// An error message from the server.
type ErrMessage struct {
	Error string
}

/////////// Auth server msgs:

// Message containing a nonce from auth-server.
type NonceMessage struct {
	Nonce string
	N     int64 // PoW difficulty: number of zeroes expected at end of md5(nonce+secret)
}

// Message containing an the secret value from client to auth-server.
type SecretMessage struct {
	Secret string
}

// Message with details for contacting the fortune-server.
type FortuneInfoMessage struct {
	FortuneServer string // TCP ip:port for contacting the fserver
	FortuneNonce  int64
}

/////////// Fortune server msgs:

// Message requesting a fortune from the fortune-server.
type FortuneReqMessage struct {
	FortuneNonce int64
}

// Response from the fortune-server containing the fortune.
type FortuneMessage struct {
	Fortune string
	Rank    int64 // Rank of this client solution
}

// Main workhorse method.
func main() {
	// TODO

	// Use json.Marshal json.Unmarshal for encoding/decoding to servers

}

// Returns the MD5 hash as a hex string for the (nonce + secret) value.
func computeNonceSecretHash(nonce string, secret string) string {
	h := md5.New()
	h.Write([]byte(nonce + secret))
	str := hex.EncodeToString(h.Sum(nil))
	return str
}
