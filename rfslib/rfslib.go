/*
This package specifies the application's interface to the distributed
records system (RFS) to be used in project 1 of UBC CS 416 2018W1.

You are not allowed to change this API, but you do have to implement
it.

*/

package rfslib

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"time"

	"github.com/DistributedClocks/GoVector/govec"
)

const (
	Insufficient = iota
	NotConfirmed
	FileDoesNotExist
	FileMaxLenReached
)

// A Record is the unit of file access (reading/appending) in RFS.
type Record [512]byte

////////////////////////////////////////////////////////////////////////////////////////////
// <ERROR DEFINITIONS>

// These type definitions allow the application to explicitly check
// for the kind of error that occurred. Each API call below lists the
// errors that it is allowed to raise.
//
// Also see:
// https://blog.golang.org/error-handling-and-go
// https://blog.golang.org/errors-are-values

// DisconnectedError ... Contains minerAddr
type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("RFS: Disconnected from the miner [%s]", string(e))
}

// BadFilenameError ... contains filename. The *only* constraint on filenames in RFS is
// that must be at most 64 bytes long.
type BadFilenameError string

func (e BadFilenameError) Error() string {
	return fmt.Sprintf("RFS: Filename [%s] has the wrong length", string(e))
}

// FileDoesNotExistError ... Contains filename.
type FileDoesNotExistError string

func (e FileDoesNotExistError) Error() string {
	return fmt.Sprintf("RFS: Cannot open file [%s] in D mode as it does not exist locally", string(e))
}

// FileExistsError ... Contains filename
type FileExistsError string

func (e FileExistsError) Error() string {
	return fmt.Sprintf("RFS: Cannot create file with filename [%s] as it already exists", string(e))
}

// FileMaxLenReachedError Contains filename
type FileMaxLenReachedError string

func (e FileMaxLenReachedError) Error() string {
	return fmt.Sprintf("RFS: File [%s] has reached its maximum length", string(e))
}

// <CUSTOM ERROR DEFINITIONS>

// ErrInsufficientCreateBalance ...
type ErrInsufficientCreateBalance struct {
	Have int
	Need int
}

func (e ErrInsufficientCreateBalance) Error() string {
	return fmt.Sprintf("Create: Insufficient balance: Have: %d, Need: %d", e.Have, e.Need)
}

// ErrInsufficientAppendBalance ...
type ErrInsufficientAppendBalance struct {
	Have int
	Need int
}

func (e ErrInsufficientAppendBalance) Error() string {
	return fmt.Sprintf("Append: Insufficient balance: Have: %d, Need: %d", e.Have, e.Need)
}

// MinerRes ...
type MinerRes struct {
	Data   interface{}
	HasErr bool
	Error  int
}

var (
	// ErrCreateNotConfirmed denotes that a create file operation has not yet been confirmed
	ErrCreateNotConfirmed = errors.New("Create file not confirmed")

	// ErrAppendNotConfirmed denotes that an append file operation has not yet been confirmed
	ErrAppendNotConfirmed = errors.New("Append file not confirmed")
)

// </CUSTOM ERROR DEFINITIONS >

// </ERROR DEFINITIONS>
////////////////////////////////////////////////////////////////////////////////////////////

// RFS ... Represents a connection to the RFS system.
type RFS interface {
	// Creates a new empty RFS file with name fname.
	// Requires record coins.
	//
	// Can return the following errors:
	// - DisconnectedError
	// - FileExistsError
	// - BadFilenameError
	CreateFile(fname string) (err error)

	// Returns a slice of strings containing filenames of all the
	// existing files in RFS.
	//
	// Can return the following errors:
	// - DisconnectedError
	ListFiles() (fnames []string, err error)

	// Returns the total number of records in a file with filename
	// fname.
	//
	// Can return the following errors:
	// - DisconnectedError
	// - FileDoesNotExistError
	TotalRecs(fname string) (numRecs uint16, err error)

	// Reads a record from file fname at position recordNum into
	// memory pointed to by record. Returns a non-nil error if the
	// read was unsuccessful. If a record at this index does not yet
	// exist, this call must block until the record at this index
	// exists, and then return the record.
	//
	// Can return the following errors:
	// - DisconnectedError
	// - FileDoesNotExistError
	ReadRec(fname string, recordNum uint16, record *Record) (err error)

	// Appends a new record to a file with name fname with the
	// contents pointed to by record. Returns the position of the
	// record that was just appended as recordNum. Returns a non-nil
	// error if the operation was unsuccessful.
	// Requires record coins.
	//
	// Can return the following errors:
	// - DisconnectedError
	// - FileDoesNotExistError
	// - FileMaxLenReachedError
	AppendRec(fname string, record *Record) (recordNum uint16, err error)
}

// OperationRecord is a file operation on the block chain
type OperationRecord struct {
	RecordData    Record // rfslib operation data
	OperationType string // rfslib operation type (one of ["append", "create", "delete"])
	FileName      string // The name of file being operated
	RecordNum     uint16 // The chunk number of the file
	MinerID       string // An identifier that specifies the miner identifier whose record coins sponsor this operation
}

type rfsClient struct {
	*rpc.Client
	localAddr      string
	minerAddr      string
	appendCoinCost int
	createCoinCost int
}

// CreateFile implements RFS.CreateFile.
// See RFS.CreateFile.
func (client *rfsClient) CreateFile(fname string) (err error) {

	// First make sure that the filename is valid.
	if len(fname) > 64 {
		log.Printf("could not create file with fname %s because fname exceeded 64 bytes\n", fname)
		return BadFilenameError(fname)
	}

	// Get all file names first, so we can make sure that
	// fname doesn't already exist.
	fnames, err := client.ListFiles()
	if err != nil {
		if err == rpc.ErrShutdown {
			// If the RPC connection was lost, return a rfslib.DisconnectedError.
			log.Printf("connection to miner at address %s was lost, returning DisconnectedError\n", client.minerAddr)
			log.Println(err)
			return DisconnectedError(fmt.Sprintf("client disconnected from miner at %s\n", client.minerAddr))
		}
	}
	for _, fn := range fnames {
		if fn == fname {
			// This file already exists!
			log.Printf("file %s already exists, returning FileExistsError\n", fname)
			return FileExistsError(fname)
		}
	}

	// Create a create record.
	op := OperationRecord{
		OperationType: "create",
		FileName:      fname,
	}

	// First, block until we have enough record coins.
	for {
		log.Println("submitting create file record to miner...")
		var minerRes MinerRes

		err = client.Call("ClientAPI.SubmitRecord", op, &minerRes)
		if err != nil {
			if err == rpc.ErrShutdown {
				// If the RPC connection was lost, return a rfslib.DisconnectedError.
				log.Printf("connection to miner at address %s was lost, returning DisconnectedError\n", client.minerAddr)
				log.Println(err)
				return DisconnectedError(fmt.Sprintf("client disconnected from miner at %s\n", client.minerAddr))
			}
			log.Println("received unexpected server err, trying again")
			log.Println(err)
			continue
		}

		if hasErr, err := minerRes.HasErr, minerRes.Error; hasErr {
			if err == Insufficient {
				// If we don't have enough record coins to create the file, try again until we do.
				log.Println("Insufficient record coins to create file, trying again...")
				time.Sleep(500 * time.Millisecond)
				continue
			}
			// There was some other error returned by the miner, continue.
			log.Println("received unexpected server err, trying again")
			log.Println(err)
			continue
		}
		break
	}

	log.Println("create file record submitted (miner had enough coins)")

	// Now, block until the transaction is confirmed.
	for {
		log.Println("waiting until transaction is confirmed...")
		var minerRes MinerRes
		err = client.Call("ClientAPI.ConfirmOperation", op, &minerRes)
		time.Sleep(500 * time.Millisecond)
		if err != nil {
			if err == rpc.ErrShutdown {
				// If the RPC connection was lost, return a rfslib.DisconnectedError.
				log.Printf("connection to miner at address %s was lost, returning DisconnectedError\n", client.minerAddr)
				log.Println(err)
				return DisconnectedError(fmt.Sprintf("client disconnected from miner at %s\n", client.minerAddr))
			}
			log.Println("Operation not confirmed, trying again...")
			log.Println(err)
			continue
		}

		if hasErr, err := minerRes.HasErr, minerRes.Error; hasErr {
			if err == NotConfirmed {
				// We're not confirmed yet, try again until we are.
				log.Println("Operation not confirmed, trying again...")
				time.Sleep(500 * time.Millisecond)
				continue
			}
			// There was some other error returned by the miner, continue.
			log.Println("received unexpected server err, trying again")
			log.Println(err)
			continue
		}
		break
	}

	// If we got this far, the operation was submitted and confirmed.
	return nil
}

// ListFiles implements RFS.ListFiles.
// See RFS.ListFiles.
func (client *rfsClient) ListFiles() (fnames []string, err error) {
	for {
		log.Println("asking miner to list all files...")
		var reply []string
		err = client.Call("ClientAPI.ListFiles", client.localAddr, &reply)
		if err != nil {
			if err == rpc.ErrShutdown {
				// If the RPC connection was lost, return a rfslib.DisconnectedError.
				log.Printf("connection to miner at address %s was lost, returning DisconnectedError\n", client.minerAddr)
				log.Println(err)
				return nil, DisconnectedError(fmt.Sprintf("client disconnected from miner at %s\n", client.minerAddr))
			}
			// Otherwise, the error was something else, server related.
			// We need to infinitely retry the remote call until either
			// (a) a response is successfully received, or
			// (b) we encounter a disconnection error.
			log.Println("miner encountered some other error, try again")
			log.Println(err)
			continue
		}
		return reply, nil
	}
}

// TotalRecs implements RFS.TotalRecs.
// See RFS.TotalRecs.
func (client *rfsClient) TotalRecs(fname string) (numRecs uint16, err error) {
	for {
		log.Printf("asking miner to count all records in file %s...\n", fname)
		var minerRes MinerRes
		err = client.Call("ClientAPI.CountRecords", fname, &minerRes)
		if err != nil {
			if err == rpc.ErrShutdown {
				// If the RPC connection was lost, return a rfslib.DisconnectedError.
				log.Printf("connection to miner at address %s was lost, returning DisconnectedError\n", client.minerAddr)
				log.Println(err)
				return 0, DisconnectedError(fmt.Sprintf("client disconnected from miner at %s\n", client.minerAddr))
			}
			log.Println("miner encountered some other error, try again")
			log.Println(err)
			continue
		}

		if hasErr, err := minerRes.HasErr, minerRes.Error; hasErr {
			if err == FileDoesNotExist {
				// If the file does not exist (according to our miner connection),
				// return FileDoesNotExistError.
				log.Printf("file %s does not exist, returning FileDoesNotExistError\n", fname)
				return 0, FileDoesNotExistError(fname)
			}
			// There was some other error returned by the miner, continue.
			log.Println("received unexpected server err, trying again")
			log.Println(err)
			continue
		}

		return minerRes.Data.(uint16), nil
	}
}

// ReadRec implements RFS.ReadRec.
// See RFS.ReadRec.
func (client *rfsClient) ReadRec(fname string, recordNum uint16, record *Record) (err error) {

	// Get all file names first, so we can make sure that
	// fname exists.
	log.Printf("before reading record, ensuring that file %s exists...\n", fname)
	fnames, err := client.ListFiles()
	if err != nil {
		if err == rpc.ErrShutdown {
			// If the RPC connection was lost, return a rfslib.DisconnectedError.
			log.Printf("connection to miner at address %s was lost, returning DisconnectedError\n", client.minerAddr)
			log.Println(err)
			return DisconnectedError(fmt.Sprintf("client disconnected from miner at %s\n", client.minerAddr))
		}
	}

	found := false
	for _, fn := range fnames {
		if fn == fname {
			// This file exists!
			found = true
		}
	}

	if !found {
		log.Printf("file %s does not exist, returning FileDoesNotExistError\n", fname)
		return FileDoesNotExistError(fmt.Sprintf("file %s does not exist", fname))
	}

	// Read the record in fname at recordNum
	op := OperationRecord{
		FileName:  fname,
		RecordNum: recordNum,
	}

	for {
		log.Printf("asking miner to read record of file %s at position %d...\n", fname, recordNum)
		var minerRes *MinerRes
		err = client.Call("ClientAPI.ReadRecord", op, &minerRes)
		if err != nil {
			if err == rpc.ErrShutdown {
				// If the RPC connection was lost, return a rfslib.DisconnectedError.
				log.Printf("connection to miner at address %s was lost, returning DisconnectedError\n", client.minerAddr)
				log.Println(err)
				return DisconnectedError(fmt.Sprintf("client disconnected from miner at %s\n", client.minerAddr))
			}
			// On some other error, just try again.
			log.Println("miner encountered some other error, try again")
			log.Println(err)
			continue
		}

		if hasErr, err := minerRes.HasErr, minerRes.Error; hasErr {
			if err == FileDoesNotExist {
				// If the file does not exist (according to our miner connection),
				// return FileDoesNotExistError.
				log.Printf("file %s does not exist, returning FileDoesNotExistError\n", fname)
				return FileDoesNotExistError(fname)
			}
			// There was some other error returned by the miner, continue.
			log.Println("received unexpected server err, trying again")
			log.Println(err)
			continue
		}

		gotNum := minerRes.Data.(OperationRecord).RecordNum
		if recordNum == gotNum {
			// We found the corrent record.  Read the data into
			// the record pointer and return.
			*record = minerRes.Data.(OperationRecord).RecordData
			return
		}
		// Otherwise, we got an incorrent record.  This probably means
		// that recordNum has not been confirmed yet.  Just try again.
		log.Printf("miner returned record with serial num %d, expected %d\n", recordNum, minerRes.Data.(OperationRecord).RecordData)
		log.Println("trying again...")
		time.Sleep(500 * time.Millisecond)
	}
}

// AppendRec implements RFS.AppendRec.
// See RFS.AppendRec.
func (client *rfsClient) AppendRec(fname string, record *Record) (recordNum uint16, err error) {

	// Get all file names first, so we can make sure that
	// fname exists.
	log.Printf("before reading record, ensuring that file %s exists...\n", fname)
	fnames, err := client.ListFiles()
	if err != nil {
		if err == rpc.ErrShutdown {
			// If the RPC connection was lost, return a rfslib.DisconnectedError.
			log.Printf("connection to miner at address %s was lost, returning DisconnectedError\n", client.minerAddr)
			log.Println(err)
			return 0, DisconnectedError(fmt.Sprintf("client disconnected from miner at %s\n", client.minerAddr))
		}
	}

	found := false
	for _, fn := range fnames {
		if fn == fname {
			// This file exists!
			found = true
		}
	}

	if !found {
		log.Printf("file %s does not exist, returning FileDoesNotExistError\n", fname)
		return 0, FileDoesNotExistError(fmt.Sprintf("file %s does not exist", fname))
	}

	// Create an append record.
	op := OperationRecord{
		OperationType: "append",
		FileName:      fname,
		RecordData:    *record,
	}

	// First, block until we have enough record coins to append.
	var minerRes MinerRes
	for {
		log.Printf("asking miner to append record to file %s\n", fname)
		err = client.Call("ClientAPI.SubmitRecord", op, &minerRes)
		if err != nil {
			if err == rpc.ErrShutdown {
				// If the RPC connection was lost, return a rfslib.DisconnectedError.
				log.Printf("connection to miner at address %s was lost, returning DisconnectedError\n", client.minerAddr)
				log.Println(err)
				return 0, DisconnectedError(fmt.Sprintf("client disconnected from miner at %s\n", client.minerAddr))
			}
			// There was some other error returned by the miner, continue.
			log.Println("received unexpected server err, trying again")
			log.Println(err)
			continue
		}

		if hasErr, err := minerRes.HasErr, minerRes.Error; hasErr {
			switch err {
			case Insufficient:
				// If we don't have enough record coins to append to the file, try again until we do.
				log.Println("Insufficient record coins to append to file, trying again...")
				time.Sleep(500 * time.Millisecond)
				continue
			case FileMaxLenReached:
				log.Printf("could not append to %s because its max length has been reached", fname)
				return 0, FileMaxLenReachedError(fname)
			default:
				// If there was some other error, also just try again.
				log.Println("miner encountered some other error, try again")
				continue
			}
		}
		break
	}

	log.Println("append file record submitted (miner had enough coins)")

	// Now, block until the transaction is confirmed.
	for {
		log.Println("waiting until transaction is confirmed...")
		var minerRes MinerRes
		err = client.Call("ClientAPI.ConfirmOperation", op, &minerRes)
		if err != nil {
			if err == rpc.ErrShutdown {
				// If the RPC connection was lost, return a rfslib.DisconnectedError.
				log.Printf("connection to miner at address %s was lost, returning DisconnectedError\n", client.minerAddr)
				log.Println(err)
				return 0, DisconnectedError(fmt.Sprintf("client disconnected from miner at %s\n", client.minerAddr))
			}
			// There was some other error returned by the miner, continue.
			log.Println("received unexpected server err, trying again")
			log.Println(err)
			continue
		}

		if hasErr, err := minerRes.HasErr, minerRes.Error; hasErr {
			if err == NotConfirmed {
				// We're not confirmed yet, try again until we are.
				log.Println("Operation not confirmed, trying again...")
				time.Sleep(500 * time.Millisecond)
				continue
			}
			// If there was some other error, also just try again.
			log.Println("miner encountered some other error, try again")
			log.Println(err)
			continue
		}
		break
	}
	// If we got this far, the operation was submitted and confirmed.
	return minerRes.Data.(uint16), nil
}

// Initialize ...
// The constructor for a new RFS object instance. Takes the miner's
// IP:port address string as parameter, and the localAddr which is the
// local IP:port to use to establish the connection to the miner.
//
// The returned rfs instance is singleton: an application is expected
// to interact with just one rfs at a time.
//
// This call should only succeed if the connection to the miner
// succeeds. This call can return the following errors:
// - Networking errors related to localAddr or minerAddr
func Initialize(localAddr string, minerAddr string) (rfs RFS, err error) {
	//Initalize GoVector
	// options := govec.GetDefaultLogOptions()
	//Access config and set timestamps (realtime) to true
	config := govec.GetDefaultConfig()
	config.UseTimestamps = true
	// logger := govec.InitGoVector("Client", "clientlog", config)

	// client, err := vrpc.RPCDial("tcp", minerAddr, logger, options)
	client, err := rpc.Dial("tcp", minerAddr)
	if err != nil {
		log.Println(err)
		return nil, DisconnectedError(minerAddr)
	}

	log.Printf("initialized client at localAddr %s and minerAddr %s\n", localAddr, minerAddr)

	return &rfsClient{
		Client:    client,
		localAddr: localAddr,
		minerAddr: minerAddr,
	}, nil
}
