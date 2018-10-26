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
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"github.ugrad.cs.ubc.ca/CPSC416-2018W-T1/P1-i8b0b-e8y0b/rfslib"
	// TODO
)

// AbstractBlock is the interface of GenesisBlock, NOPBlock and OPBlock
type AbstractBlock interface {
}

// ActualBlock is the "base class" of GenesisBlock, NOPBlock and OPBlock
type ActualBlock struct {
	Block   AbstractBlock
	MinerID string // The identifier of the miner that computed this block (block-minerID)
}

// GenesisBlock is the first block in this blockchain
type GenesisBlock struct {
	Hash string // The genesis (first) block MD5 hash for this blockchain
}

// NOPBlock is a No-OP Block
type NOPBlock struct {
	PrevHash string // A hash of the previous block in the chain (prev-hash)
	Nonce    uint32 // A 32-bit unsigned integer nonce (nonce)
}

// OPBlock is a OP Block with non-empty Records
type OPBlock struct {
	PrevHash string            // A hash of the previous block in the chain (prev-hash)
	Records  []OperationRecord // An ordered set of operation records
	Nonce    uint32            // A 32-bit unsigned integer nonce (nonce)
}

// OperationRecord is a file operation on the block chain
type OperationRecord struct {
	RecordData    rfslib.Record // rfslib operation data
	OperationType string        // rfslib operation type (one of ["append", "create"])
	RecordNum     uint16
	MinerID       string // An identifier that specifies the miner identifier whose record coins sponsor this operation (op-minerID)
}

// Miner mines blocks.
type Miner struct {
	Settings
}

// Settings contains all miner settings and is loaded through
// a configuration file.  See:
// https://www.cs.ubc.ca/~bestchai/teaching/cs416_2018w1/project1/config.json.
type Settings struct {
	// The ID of this miner (max 16 characters).
	MinerID string

	// An array of remote IP:port addresses, one per peer miner that this miner should
	// connect to (using the OutgoingMinersIP below).
	PeerMinersAddrs []string

	// The local IP:port where the miner should expect other miners to connect to it
	// (address it should listen on for connections from miners).
	IncomingMinersAddr string

	// The local IP that the miner should use to connect to peer miners.
	OutgoingMinersIP string

	// The local IP:port where this miner should expect to receive connections
	// from RFS clients (address it should listen on for connections from clients)
	IncomingClientsAddr string

	// The number of record coins mined for an op block.
	MinedCoinsPerOpBlock uint8

	// The number of record coins mined for a no-op block.
	MinedCoinsPerNoOpBlock uint8

	// The number of record coins charged for creating a file.
	NumCoinsPerFileCreate uint8

	// Time in milliseconds, the minimum time between op block mining.
	GenOpBlockTimeout uint8

	// The genesis (first) block MD5 hash for this blockchain.
	GenesisBlockHash string

	// The op block difficulty (proof of work setting: number of zeroes).
	PowPerOpBlock uint8

	// The no-op block difficulty (proof of work setting: number of zeroes).
	PowPerNoOpBlock uint8

	// The number of confirmations for a create file operation
	// (the number of blocks that must follow the block containing a create file operation
	// along longest chain before the CreateFile call can return successfully).
	ConfirmsPerFileCreate uint8

	// The number of confirmations for an append operation (the number of blocks
	// that must follow the block containing an append operation along longest chain
	// before the AppendRec call can return successfully). Note that this append confirm
	// number will always be set to be larger than the create confirm number (above).
	ConfirmsPerFileAppend uint8
}

// ClientAPI is the set of RPC calls provided to RFS
type ClientAPI struct {
	IncomingClientsAddr string // The local IP:port where this miner should expect to receive connections from RFS clients (address it should listen on for connections from clients)
}

// MinerAPI is the set of RPC calls provided to other miners
type MinerAPI struct {
	PeerMinersAddrs    []string // An array of remote IP:port addresses, one per peer miner that this miner should connect to (using the OutgoingMinersIP below)
	IncomingMinersAddr string   // The local IP:port where the miner should expect other miners to connect to it (address it should listen on for connections from miners)
}

var chain map[string]OPBlock
var unconfirmedOperations []OperationRecord

/*
func (t *Miner) GetChainTips(args *Args, quo *Quotient) error {
	return nil
}
*/

// validateBlock returns true if the given block is valid, false otherwise
func validateBlock(block AbstractBlock) bool {
	// Block validations
	// Check that the nonce for the block is valid: PoW is correct and has the right difficulty.
	// Check that the previous block hash points to a legal, previously generated, block.

	// Operation validations:
	// Check that each operation in the block is associated with a miner ID that has enough record coins to pay for the operation
	// (i.e., the number of record coins associated with the minerID must have sufficient balance to 'pay' for the operation).

	// Check that each operation does not violate RFS semantics
	// (e.g., a record is not mutated or inserted into the middled of an rfs file).

	return true
}

// computeNonceSecretHash returns the MD5 hash as a hex string for the (nonce + secret) value.
func computeNonceSecretHash(nonce string, secret string) string {
	h := md5.New()
	h.Write([]byte(nonce + secret))
	str := hex.EncodeToString(h.Sum(nil))
	return str
}

// SubmitBlock is an RPC call invoked by other Miner instances
// it would validate the given block and accept it if it is valid
func (mapi *MinerAPI) SubmitBlock(blockPacket *ActualBlock, status *bool) error {
	if validateBlock(blockPacket) == false {
		*status = false
		return nil
	}
	// switch t := blockPacket.Block.(type) {
	// default:
	// 	return errors.New("Invalid Block Type")
	// case OPBlock:
	// 	fmt.Println("Got OPBlock") // t has type OPBlock
	// case NOPBlock:
	// 	fmt.Println("Got NOPBlock") // t has type NOPBlock
	// case GenesisBlock:
	// 	fmt.Println("Got GenesisBlock") // t has type GenesisBlock
	// }
	// quo.Quo = args.A / args.B
	// quo.Rem = args.A % args.B
	*status = true
	return nil
}

func (m *Miner) computeNOPBlock() (NOPBlock, error) {
	// minerID := ""
	prevHash := ""
	var nounce uint32
	nounce = 1
	nopBlock := NOPBlock{prevHash, nounce}
	return nopBlock, nil
}

func computeOPBlock() error {
	return nil
}

func broadcastBlock(block ActualBlock, peerMinersAddrs []string) {
	for _, addr := range peerMinersAddrs {
		// go
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		// Then it can make a remote call:

		// Synchronous call
		args := &block
		var reply bool
		err = client.Call("MinerAPI.SubmitBlock", args, &reply)
		if err != nil {
			log.Fatal("SubmitBlock error:", err)
		}
		fmt.Println("SubmitBlock successed:", reply)
	}

}

func initializeBlockChain(genesisBlockHash string, peerMinersAddrs []string) error {
	// genesisBlock := GenesisBlock{genesisBlockHash}

	// broadcastBlock(genesisBlock, peerMinersAddrs)
	return nil
}

// loadJSON loads a settings json file and populates
// a Settings struct with the data it reads.
func loadJSON(fn string) (Settings, error) {
	// Get a file descriptor for the specified file.
	fi, err := os.Open(fn)
	if err != nil {
		return Settings{}, err
	}

	// Decode the contents of the file into a
	// Settings struct.
	var s Settings
	err = json.NewDecoder(fi).Decode(&s)
	if err != nil {
		return Settings{}, err
	}

	return s, nil
}

// Main workhorse method.  We are exposing two sets of APIs through RPC;
// one for other miners, and one for clients.
func main() {

	// Make sure cmd line input is correct.
	if len(os.Args) != 2 {
		log.Fatalln("Usage: go run miner.go </path/to/config.json>")
	}

	// Load up the settings from the specified json file.
	jsonFn := os.Args[1]
	settings, err := loadJSON(jsonFn)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("settings", settings)

	// Register RPC methods for other miners to call.
	minerAPI := new(MinerAPI)
	rpc.Register(minerAPI)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", minerAPI.IncomingMinersAddr)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

	// Register RPC methods for clients to call.
	clientAPI := new(ClientAPI)
	rpc.Register(clientAPI)
	rpc.HandleHTTP()
	l, e = net.Listen("tcp", clientAPI.IncomingClientsAddr)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
