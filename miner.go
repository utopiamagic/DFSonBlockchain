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

// Block is the base class of GenesisBlock, NOPBlock and OPBlock
type Block struct {
	MinerID string // The identifier of the miner that computed this block (block-minerID)
}

// GenesisBlock is the first block in this blockchain
type GenesisBlock struct {
	Block
	Hash string // The genesis (first) block MD5 hash for this blockchain
}

// NOPBlock is a No-OP Block
type NOPBlock struct {
	Block
	PrevHash string // A hash of the previous block in the chain (prev-hash)
	Nonce    uint32 //A 32-bit unsigned integer nonce (nonce)
}

// OPBlock is a OP Block with non-empty Records
type OPBlock struct {
	Block
	PrevHash string            // A hash of the previous block in the chain (prev-hash)
	Records  []OperationRecord // An ordered set of operation records
	Nonce    uint32            //A 32-bit unsigned integer nonce (nonce)
}

// OperationRecord is a file operation on the block chain
type OperationRecord struct {
	RecordData    rfslib.Record // rfslib operation data
	OperationType string        // rfslib operation type (one of ["append", "create"])
	RecordNum     uint16
	MinerID       string // An identifier that specifies the miner identifier whose record coins sponsor this operation (op-minerID)
}

// Miner information is loaded through the configuration file
type Miner struct {
	MinerID             string   // The ID of this miner (max 16 characters).
	PeerMinersAddrs     []string // An array of remote IP:port addresses, one per peer miner that this miner should connect to (using the OutgoingMinersIP below)
	IncomingMinersAddr  string   // The local IP:port where the miner should expect other miners to connect to it (address it should listen on for connections from miners)
	OutgoingMinersIP    string   // The local IP that the miner should use to connect to peer miners
	IncomingClientsAddr string   // The local IP:port where this miner should expect to receive connections from RFS clients (address it should listen on for connections from clients)

	MinedCoinsPerOpBlock   uint8  // The number of record coins mined for an op block
	MinedCoinsPerNoOpBlock uint8  // The number of record coins mined for a no-op block
	NumCoinsPerFileCreate  uint8  // The number of record coins charged for creating a file
	GenOpBlockTimeout      uint8  // Time in milliseconds, the minimum time between op block mining (see diagram above).
	GenesisBlockHash       string // The genesis (first) block MD5 hash for this blockchain
	PowPerOpBlock          uint8  // The op block difficulty (proof of work setting: number of zeroes)
	PowPerNoOpBlock        uint8  // The no-op block difficulty (proof of work setting: number of zeroes)
	ConfirmsPerFileCreate  uint8  // The number of confirmations for a create file operation (the number of blocks that must follow the block containing a create file operation along longest chain before the CreateFile call can return successfully)
	ConfirmsPerFileAppend  uint8  // The number of confirmations for an append operation (the number of blocks that must follow the block containing an append operation along longest chain before the AppendRec call can return successfully). Note that this append confirm number will always be set to be larger than the create confirm number (above).
}

var chain map[string]OPBlock

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

/*
func (t *Miner) GetChainTips(args *Args, quo *Quotient) error {
	return nil
}
*/

// validateBlock returns true if the given block is valid, false otherwise
func validateBlock(block *Block) bool {
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

// Returns the MD5 hash as a hex string for the (nonce + secret) value.
func computeNonceSecretHash(nonce string, secret string) string {
	h := md5.New()
	h.Write([]byte(nonce + secret))
	str := hex.EncodeToString(h.Sum(nil))
	return str
}

// SubmitBlock is an RPC call invoked by other Miner instances
// it would validate the given block and accept it if it is valid
func (t *Miner) SubmitBlock(block *Block, status *bool) error {
	if validateBlock(block) == false {
		*status = false
		return nil
	}
	// quo.Quo = args.A / args.B
	// quo.Rem = args.A % args.B
	*status = true
	return nil
}

func computeNOPBlock() error {
	return nil
}

func computeOPBlock() error {
	return nil
}

func initializeBlockChain(genesisBlockHash string, peerMinersAddrs []string) error {

}

// Main workhorse method.
func main() {
	// TODO

	// Use json.Marshal json.Unmarshal for encoding/decoding to servers

}
