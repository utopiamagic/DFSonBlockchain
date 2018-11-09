Coming up soon
==============

## ClientAPI

## MinerAPI

### func (*MinerAPI) GetChainTips
The `GetChainTips` RPC returns information about the highest-height block (tip) of each local block chain
```
func (mapi *MinerAPI) GetChainTips(caller string, reply *map[string]int) error
``` 

### func (*MinerAPI) GetBlock
The `GetBlock` RPC gets a block with a particular header hash from the local block hash map as a `BlockPacket`
```
func (mapi *MinerAPI) GetBlock(headerHash string, packet *BlockPacket) error
``` 

### func (*MinerAPI) AddNode
The `AddNode` RPC attempts to try a connection to a `Miner` node once
```
func (mapi *MinerAPI) AddNode(minerInfo PeerMinerInfo, hasInternalError *bool) error
```

### func (*MinerAPI) GetPeerInfo
The `GetPeerInfo` RPC returns the `MinerID` of the connected `Miner` node
```
func (mapi *MinerAPI) GetPeerInfo(caller string, minerID *string) error
```

### func (*MinerAPI) SubmitRecord
The `SubmitRecord` RPC accepts an `OperationRecord`, verifies it is a valid addition to the block chain, and broadcasts it to the network 
```
func (mapi *MinerAPI) SubmitRecord(newOpRecord *rfslib.OperationRecord, hasInternalError *bool) error
```

### func (*MinerAPI) SubmitBlock
The `SubmitBlock` RPC accepts a `BlockPacket`, verifies it is a valid addition to the block chain, and broadcasts it to the network. 
```
func (mapi *MinerAPI) SubmitBlock(packet BlockPacket, hasInternalError *bool) error
```
