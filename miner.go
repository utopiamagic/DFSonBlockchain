/*
Implements a miner network

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
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"time"

	"github.com/DistributedClocks/GoVector/govec"
	"github.com/DistributedClocks/GoVector/govec/vrpc"

	"github.ugrad.cs.ubc.ca/CPSC416-2018W-T1/P1-i8b0b-e8y0b/rfslib"
)

// Block is the interface for GenesisBlock, NOPBlock and OPBlock
type Block interface {
	hash() string
	prevHash() string
	minerID() string
	// getHeight() (uint32, error)
}

// GenesisBlock is the first block in this blockchain
type GenesisBlock struct {
	Hash    string // The genesis (first) block MD5 hash for this blockchain
	MinerID string // The identifier of the miner that computed this block (block-minerID)
}

func (gblock GenesisBlock) hash() string {
	return gblock.Hash
}

func (gblock GenesisBlock) prevHash() string {
	return gblock.Hash
}

func (gblock GenesisBlock) minerID() string {
	return gblock.MinerID
}

// NOPBlock is a No-OP Block
type NOPBlock struct {
	PrevHash     string // A hash of the previous block in the chain (prev-hash)
	Nonce        uint32 // A 32-bit unsigned integer nonce (nonce)
	MinerID      string // The identifier of the miner that computed this block (block-minerID)
	MinerBalance uint32 // The updated balance of the miner that computed this block
}

func (nopblock NOPBlock) hash() string {
	h := md5.New()
	h.Write([]byte(fmt.Sprintf("%v", nopblock)))
	return hex.EncodeToString(h.Sum(nil))
}

func (nopblock NOPBlock) prevHash() string {
	return nopblock.PrevHash
}

func (nopblock NOPBlock) minerID() string {
	return nopblock.MinerID
}

// OPBlock is a OP Block with non-empty Records
type OPBlock struct {
	PrevHash     string            // A hash of the previous block in the chain (prev-hash)
	Records      []OperationRecord // An ordered set of operation records
	Nonce        uint32            // A 32-bit unsigned integer nonce (nonce)
	MinerID      string            // The identifier of the miner that computed this block (block-minerID)
	MinerBalance uint32            // The updated balance of the miner that computed this block
}

func (opblock OPBlock) hash() string {
	h := md5.New()
	h.Write([]byte(fmt.Sprintf("%v", opblock)))
	return hex.EncodeToString(h.Sum(nil))
}

func (opblock OPBlock) prevHash() string {
	return opblock.PrevHash
}

func (opblock OPBlock) minerID() string {
	return opblock.MinerID
}

// OperationRecord is a file operation on the block chain
type OperationRecord struct {
	RecordData    rfslib.Record // rfslib operation data
	OperationType string        // rfslib operation type (one of ["append", "create"])
	FileName      string        // The name of file being operated
	RecordNum     uint16        // The chunk number of the file
	MinerID       string        // An identifier that specifies the miner identifier whose record coins sponsor this operation
}

// Miner mines blocks.
type Miner struct {
	Settings

	chain                 map[string]Block  // All the blocks in the network
	unconfirmedOperations []OperationRecord // Unconfirmed operations from the miner's clients
	chainTips             []ChainTip        // The collection of the head of all valid forks
	Logger                *govec.GoLog      // The GoVector Logger
	Balance               uint32            // The current balance of the miner
	GeneratedBlocksChan   chan Block        // Channel of generated blocks
	OPBlockStopChan       chan string       // Channel that stops computeOPBlock
	NOPBlockStopChan      chan string       // Channel that stops computeNOPBlock
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
	IncomingClientsAddr string // The local IP:port where this miner should expect to receive connections from RFS clients
}

// MinerAPI is the set of RPC calls provided to other miners
type MinerAPI struct {
	PeerMinersAddrs    []string // An array of remote IP:port addresses, one per peer miner that this miner should connect to
	IncomingMinersAddr string   // The local IP:port where the miner should expect other miners to connect to it
}

// ChainTip is ...
type ChainTip struct {
	length int
	Block
}

var miner Miner

// GetChainTips RPC provides the active starting point of the current blockchain
// parameter arg is optional and not being used at all
func (mapi *MinerAPI) GetChainTips(arg interface{}, reply *[]ChainTip) error {
	*reply = miner.chainTips
	return nil
}

// GetBlock RPC gets a block with a particular header hash from the local blockchain map
func (mapi *MinerAPI) GetBlock(headerHash string, reply *Block) error {
	block, exists := miner.chain[headerHash]
	if exists {
		*reply = block
		return nil
	}
	return errors.New("The requested block does not exist in the local blockchain:" + miner.MinerID)
}

// checkSemantics checks that each operation does not violate RFS semantics
// (e.g., a record is not mutated or inserted into the middled of an rfs file).
func (m *Miner) checkSemantics(block Block, opRecord OperationRecord) error {
	currentRecordNum := opRecord.RecordNum
	for block.hash() != block.prevHash() {
		prevBlock, ok := m.chain[block.prevHash()]
		if !ok {
			return errors.New("Encountered an orphaned block when checking RFS semantics")
		}
		switch t := block.(type) {
		case NOPBlock:
			break
		case OPBlock:
			if opRecord.OperationType == "create" {
				for _, prevRecord := range t.Records {
					if prevRecord.FileName == opRecord.FileName {
						return errors.New("The given file name already exists in this chain")
					}
				}
			} else if opRecord.OperationType == "append" {
				for _, prevRecord := range t.Records {
					if currentRecordNum == prevRecord.RecordNum+1 {
						currentRecordNum = prevRecord.RecordNum
						if currentRecordNum == 0 {
							// TODO: decide the initial record num for append
							return nil
						}
					} else {
						return errors.New("Encountered an invalid OperationRecord (inserted into the middled of an rfs file)")
					}
				}
			} else {
				return errors.New("Encountered an invalid OperationRecord Type")
			}
		default:
			return errors.New("Encountered an invalid intermediate block when checking RFS semantics")
		}
		block = prevBlock
	}
	return nil
}

// validateBlock returns true if the given block is valid, false otherwise
func (m *Miner) validateBlock(block Block) error {
	// Block validations
	switch t := block.(type) {
	default:
		return errors.New("Invalid Block Type")
	case OPBlock:
		if validateNonce(t, m.PowPerOpBlock) == false {
			return errors.New("The given OPBlock does not have the right difficulty")
		}
		// Check that the previous block hash points to a legal, previously generated, block.
		if val, ok := m.chain[t.PrevHash]; !ok {
			return errors.New("The given OPBlock does not have a previous block")
		}

		// Operation validations:
		// Check that each operation in the block is associated with a miner ID that has enough record coins to pay for the operation
		// (i.e., the number of record coins associated with the minerID must have sufficient balance to 'pay' for the operation).
		balanceRequiredMap := make(map[string]uint32)
		for _, opRecord := range t.Records {
			if opRecord.OperationType == "create" {
				balanceRequiredMap[opRecord.MinerID] += uint32(m.NumCoinsPerFileCreate)
			} else if opRecord.OperationType == "append" {
				balanceRequiredMap[opRecord.MinerID]++
			}
		}
		for minerID, balanceRequired := range balanceRequiredMap {
			balance, err := m.getBalance(t, minerID)
			if err != nil {
				return errors.New("Checking balanceRequired:" + err.Error())
			} else if balance < balanceRequired {
				return errors.New("The miner of the OperationRecord does not have enough balance")
			}
		}

		// Check that each operation does not violate RFS semantics
		// (e.g., a record is not mutated or inserted into the middled of an rfs file).
		for _, opRecord := range t.Records {
			if err := m.checkSemantics(t, opRecord); err != nil {
				return err
			}
		}
		break
	case NOPBlock:
		if validateNonce(t, m.PowPerNoOpBlock) == false {
			return errors.New("The given NOPBlock does not have the right difficulty")
		}
		// Check that the previous block hash points to a legal, previously generated, block.
		if val, ok := m.chain[t.PrevHash]; ok {
			fmt.Println("NOPBlock: the previous block is:", t.prevHash())
			m.chain[t.hash()] = t
		} else {
			return errors.New("The given NOPBlock does not have a previous block")
		}
		break
	case GenesisBlock:
		if m.chain == nil || len(m.chain) == 0 {
			break
		} else {
			return errors.New("A GenesisBlock already existed")
		}
	}
	return nil
}

// SubmitRecord RPC call should be invoked by the RFS Client
// it submits operationRecord to the miner network if the mined coins are sufficient to cover the cost
func (capi *ClientAPI) SubmitRecord(operationRecord *OperationRecord, status *bool) error {
	return nil
}

// ConfirmOperation RPC should be invoked by the RFS Client
// upon confimation it sets status to true
func (capi *ClientAPI) ConfirmOperation(operationRecord *OperationRecord, status *bool) error {
	if operationRecord.OperationType == "append" {

	} else if operationRecord.OperationType == "create" {

	}
	*status = false
	return errors.New("OperationType cannot be recognized")
}

// GetBalance RPC call should be invoked by the RFS Client and does not take an input
// it returns the current balance of the longest chain of the miner being quried
func (capi *ClientAPI) GetBalance(whatever interface{}, currentBalance *uint32) error {
	bestChainTip := miner.getBlockFromLongestChain()
	var err error
	*currentBalance, err = miner.getBalance(bestChainTip, miner.MinerID)
	return err
}

// getHeight returns the height of the given block in the local chain
func (m *Miner) getHeight(block Block) (int, error) {
	if block.hash() == block.prevHash() {
		// GenesisBlock case
		return 1, nil
	}
	// OPBlock, NOPBlock case
	prevBlock, exists := m.chain[block.hash()]
	if exists {
		prevHeight, err := m.getHeight(prevBlock)
		if err == nil {
			return prevHeight + 1, nil
		}
		return prevHeight, err
	}
	return 1, errors.New("The given NOPBlock/OPBlock starts with an orphaned block:" + block.hash())
}

func (m *Miner) requestPreviousBlocks(block Block) error {
	prevHash := block.prevHash()
	prevBlock, exists := m.chain[prevHash]
	for ; !exists; prevHash = prevBlock.prevHash() {
		prevBlock, exists = m.chain[prevHash]
		for _, addr := range m.PeerMinersAddrs {
			options := govec.GetDefaultLogOptions()
			client, err := vrpc.RPCDial("tcp", addr, m.Logger, options)
			if err != nil {
				log.Fatal("dialing:", addr, err)
				continue
			}
			// Then it can make a remote asynchronous call
			replyBlock := new(Block)
			err = client.Call("MinerAPI.GetBlock", prevHash, replyBlock)
			if err != nil {
				log.Fatal("requestPreviousBlocks error:", err)
				return err
			}
			m.chain[block.hash()] = *replyBlock
			fmt.Println("requestPreviousBlocks successed:", (*replyBlock).hash())
		}
	}
	return nil
}

func (m *Miner) updateChainTip(prevHash string) error {
	prevBlock, exists := m.chain[prevHash]
	if exists {
		inChainTips := false
		tipIndex := 0
		for i, v := range m.chainTips {
			if v.Block == prevBlock {
				inChainTips = true
				tipIndex = i
			}
		}
		if inChainTips {
			// we are the first one to work on an original branch
			m.chainTips[tipIndex] = ChainTip{m.chainTips[tipIndex].length + 1, prevBlock}
		} else {
			// this is a fork of another branch
			prevHeight, err := m.getHeight(prevBlock)
			if err != nil {
				m.chainTips = append(m.chainTips, ChainTip{prevHeight + 1, prevBlock})
			} else {
				// ask other miners for the previous block?
				return err
			}
		}
		return nil
	}
	return errors.New("Cannot find the previous block")
}

// getBalance finds the current balance along the chain starting
func (m *Miner) getBalance(block Block, minerID string) (uint32, error) {
	foundBalance := false
	var mostRecentBalance uint32
	var recentTrasactionFee uint32
	for block.hash() != block.prevHash() {
		switch t := block.(type) {
		case NOPBlock:
			if t.MinerID == minerID {
				mostRecentBalance = t.MinerBalance
				break
			}
		case OPBlock:
			if t.MinerID == minerID {
				mostRecentBalance = t.MinerBalance
				foundBalance := true
				break
			}
			for i, record := range t.Records {
				if record.MinerID == minerID {
					if record.OperationType == "append" {
						recentTrasactionFee++
					} else if record.OperationType == "create" {
						recentTrasactionFee += uint32(miner.NumCoinsPerFileCreate)
					}
				}
			}
			break
		default:
			return 0, errors.New("Encountered an unknown block")
		}
		nextBlock, exists := m.chain[block.prevHash()]
		if exists {
			block = nextBlock
		} else {
			return 0, errors.New("This chain does not have a valid head")
		}

	}
	if foundBalance {
		return mostRecentBalance - recentTrasactionFee, nil
	}
	return 0, nil
}

func (m *Miner) addBlock(block Block) error {
	err := m.validateBlock(block)
	if err == nil {
		if block.hash() == block.prevHash() {
			// GenesisBlock case
			if len(m.chainTips) == 0 {
				m.chain[block.hash()] = block
				m.chainTips = append(m.chainTips, ChainTip{1, block})
			} else {
				return errors.New("The local chain already contains a GenesisBlock")
			}
		}
		err := m.updateChainTip(block.hash())
		if err != nil {
			return err
		}
		return nil
	}
	// validation error
	return err
}

// SubmitBlock is an RPC call invoked by other Miner instances
// it accepts the given block upon successful validation
func (mapi *MinerAPI) SubmitBlock(block Block, status *bool) error {
	err := miner.addBlock(block)
	if err == nil {
		*status = true
		// stop computeNOPBlock and computeNOPBlock
		// quitMining <- true
		// miner.broadcastBlock(genesisBlock)
		return nil
	}
	return err
}

func countTrailingZeros(str string) uint8 {
	var reverseCounter uint8
	for i := len(str) - 1; i >= 0; i-- {
		if str[i] == 0 {
			reverseCounter++
		} else {
			break
		}
	}
	return reverseCounter
}

// validateNonce computes the MD5 hash as a hex string for the block
// and checks if it has sufficient trailing zeros
func validateNonce(block Block, difficulty uint8) bool {
	h := md5.New()
	h.Write([]byte(fmt.Sprintf("%v", block)))
	combinedHash := hex.EncodeToString(h.Sum(nil))
	if countTrailingZeros(combinedHash) >= difficulty {
		return true
	}
	return false
}

func (m *Miner) getBlockFromLongestChain() Block {
	maxBlocksNum := 0
	var longestChainTip Block
	for _, v := range m.chainTips {
		if v.length > maxBlocksNum {
			maxBlocksNum = v.length
			longestChainTip = v
		}
	}
	return longestChainTip
}

// computeNOPBlock tries to construct a NOPBlock when it is not requested to stop
func (m *Miner) computeNOPBlock() {
	var nonce uint32
	nonce = 1
	prevHash := m.getBlockFromLongestChain().hash()
	nopBlock := NOPBlock{prevHash, nonce, m.MinerID, m.Balance + uint32(m.MinedCoinsPerNoOpBlock)}
	for {
		select {
		default:
			nopBlock = NOPBlock{prevHash, nonce, m.MinerID, m.Balance + uint32(m.MinedCoinsPerNoOpBlock)}
			if validateNonce(nopBlock, m.PowPerNoOpBlock) == true {
				m.GeneratedBlocksChan <- nopBlock
			}
			nonce++
		case processName := <-m.NOPBlockStopChan:
			log.Println("computeNOPBlock has been requested to quit by " + processName)
			return
		}
	}
}

// computeOPBlock works similarly except it takes all the records collected in the given time
func (m *Miner) computeOPBlock(records []OperationRecord) {
	var nonce uint32
	nonce = 1
	prevHash := m.getBlockFromLongestChain().hash()
	opBlock := OPBlock{prevHash, records, nonce, m.MinerID, m.Balance + uint32(m.MinedCoinsPerOpBlock)}
	for {
		select {
		default:
			opBlock = OPBlock{prevHash, records, nonce, m.MinerID, m.Balance + uint32(m.MinedCoinsPerOpBlock)}
			if validateNonce(opBlock, m.PowPerOpBlock) == true {
				m.GeneratedBlocksChan <- opBlock
			}
			nonce++
		case processName := <-m.OPBlockStopChan:
			log.Println("computeOPBlock has been requested to quit by " + processName)
			return
		}
	}
}

// generateBlocks should only be called once
func (m *Miner) generateBlocks() {
	opBlockTimer := time.NewTimer(0 * time.Millisecond)
	m.OPBlockStopChan = make(chan string)
	m.NOPBlockStopChan = make(chan string)
	m.GeneratedBlocksChan = make(chan Block)
	operationRecordChan := make(chan OperationRecord)
	var records []OperationRecord
	generatingNOPBlock := false
	for {
		select {
		default:
			if !generatingNOPBlock {
				go m.computeNOPBlock()
				generatingNOPBlock = true
			}
		case generatedBlock := <-m.GeneratedBlocksChan:
			switch generatedBlock.(type) {
			case NOPBlock:
				generatingNOPBlock = false
				log.Println("Received the generated NOPBlock")
			case OPBlock:
				log.Println("Received the generated OPBlock")
			default:
				log.Println("Received the generated block of some other type")
				break
			}
			go m.broadcastBlock(generatedBlock)
		case operationRecord := <-operationRecordChan:
			if opBlockTimer.Stop() {
				opBlockTimer.Reset(time.Duration(m.GenOpBlockTimeout) * time.Millisecond)
				m.NOPBlockStopChan <- "generateBlocks()"
			}
			records = append(records, operationRecord)
		case <-opBlockTimer.C:
			go m.computeOPBlock(records)
		}
	}
}

func (m *Miner) broadcastBlock(block Block) error {
	var calls []*rpc.Call
	var errStrings []string
	for _, addr := range m.PeerMinersAddrs {
		options := govec.GetDefaultLogOptions()
		client, err := vrpc.RPCDial("tcp", addr, miner.Logger, options)
		if err != nil {
			log.Fatal("dialing:", addr, err)
			continue
		}
		// Then it can make a remote asynchronous call
		reply := new(bool)
		submitBlockCall := client.Go("MinerAPI.SubmitBlock", block, reply, nil)
		append(calls, submitBlockCall)
	}

	for i, call := range calls {
		// do something with e.Value
		replyCall := <-call.Done // will be equal to divCall
		if replyCall.Error == nil {
			if replyCall.Reply == true {
				continue
			} else {
				append(errStrings, "submitBlockCall.Reply is false")
			}
		} else {
			errString := m.PeerMinersAddrs[i] + ": " + replyCall.Error.Error()
			append(errStrings, errString)
		}
	}

	if len(errStrings) > 0 {
		return errors.New("one or multiple submitBlockCall failed")
	}
	return nil
}

func (m *Miner) initializeBlockChain(genesisBlockHash string, peerMinersAddrs []string) error {
	genesisBlock := GenesisBlock{genesisBlockHash, m.MinerID}

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
	fmt.Println("Loaded settings", settings)

	fmt.Println("Starting miner server")

	// Register RPC methods for other miners to call.
	minerAPI := new(MinerAPI)
	minerServer := rpc.NewServer()
	minerServer.Register(minerAPI)
	l, e := net.Listen("tcp", minerAPI.IncomingMinersAddr)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	//Initalize GoVector
	options := govec.GetDefaultLogOptions()
	//Access config and set timestamps (realtime) to true
	config := govec.GetDefaultConfig()
	config.UseTimestamps = true
	logger := govec.InitGoVector("MinerProcess", "MinerLog", config)
	miner.Logger = logger
	go vrpc.ServeRPCConn(minerServer, l, logger, options)

	// Register RPC methods for clients to call.
	clientAPI := new(ClientAPI)
	clientServer := rpc.NewServer()
	clientServer.Register(clientAPI)
	l, e = net.Listen("tcp", clientAPI.IncomingClientsAddr)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// we will block here to serve our clients
	vrpc.ServeRPCConn(clientServer, l, logger, options)
}
