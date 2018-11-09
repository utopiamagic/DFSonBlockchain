Coming up soon
==============

## ClientAPI
### func (*ClientAPI) ReadRecord
```go
func (capi *ClientAPI) ReadRecord(recordInfo *rfslib.OperationRecord, minerRes *rfslib.MinerRes) error
```
The `ReadRecord` RPC reads a confirmed record with `recordInfo.FileName` at position `recordInfo.RecordNum` into memory pointed to by `minerRes`

### func (*ClientAPI) ConfirmOperation
```go
func (capi *ClientAPI) ConfirmOperation(operationRecord *rfslib.OperationRecord, minerRes *rfslib.MinerRes) error
```
The `ConfirmOperation` RPC confirms the operation by checking the `OPBlock` containing the operation is followed by some fixed number of other blocks in the longest blockchain

## MinerAPI

### func (*MinerAPI) GetChainTips
```go
func (mapi *MinerAPI) GetChainTips(caller string, reply *map[string]int) error
``` 
The `GetChainTips` RPC returns information about the highest-height block (tip) of each local block chain

### func (*MinerAPI) GetBlock
```go
func (mapi *MinerAPI) GetBlock(headerHash string, packet *BlockPacket) error
``` 
The `GetBlock` RPC gets a block with a particular header hash from the local block hash map as a `BlockPacket`

### func (*MinerAPI) AddNode
```go
func (mapi *MinerAPI) AddNode(minerInfo PeerMinerInfo, hasInternalError *bool) error
```
The `AddNode` RPC attempts to try a connection to a `Miner` node once

### func (*MinerAPI) GetPeerInfo
```go
func (mapi *MinerAPI) GetPeerInfo(caller string, minerID *string) error
```
The `GetPeerInfo` RPC returns the `MinerID` of the connected `Miner` node

### func (*MinerAPI) SubmitRecord
```go
func (mapi *MinerAPI) SubmitRecord(newOpRecord *rfslib.OperationRecord, hasInternalError *bool) error
```
The `SubmitRecord` RPC accepts an `OperationRecord`, verifies it is a valid addition to the block chain, and broadcasts it to the network 

### func (*MinerAPI) SubmitBlock
```go
func (mapi *MinerAPI) SubmitBlock(packet BlockPacket, hasInternalError *bool) error
```
The `SubmitBlock` RPC accepts a `BlockPacket`, verifies it is a valid addition to the block chain, and broadcasts it to the network. 
