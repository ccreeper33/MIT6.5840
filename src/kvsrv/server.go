package kvsrv

import (
	"sync"
)

type operationHistory struct {
	operationId int64
	value       string
}

type KVServer struct {
	mu            sync.Mutex
	KvMap         map[string]string
	lastOperation map[int64]operationHistory
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.lastOperation, args.ClerkId)
	value, ok := kv.KvMap[args.Key]
	if ok {
		reply.Value = value
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	_ = reply
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, result := kv.checkLastOperation(args.ClerkId, args.OperationId)
	switch result {
	case lastOperation:
	case newOperation:
		kv.KvMap[args.Key] = args.Value
		kv.lastOperation[args.ClerkId] = operationHistory{
			operationId: args.OperationId,
			value:       "",
		}
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastValue, result := kv.checkLastOperation(args.ClerkId, args.OperationId)
	switch result {
	case lastOperation:
		reply.Value = lastValue
	case newOperation:
		oldValue, ok := kv.KvMap[args.Key]
		if !ok {
			oldValue = ""
		}
		kv.KvMap[args.Key] = oldValue + args.Value
		reply.Value = oldValue
		kv.lastOperation[args.ClerkId] = operationHistory{
			operationId: args.OperationId,
			value:       oldValue,
		}
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.KvMap = make(map[string]string)
	kv.lastOperation = make(map[int64]operationHistory)
	return kv
}

type historyCheckResult int

const (
	lastOperation historyCheckResult = iota
	newOperation
)

func (kv *KVServer) checkLastOperation(clerkId int64, operationId int64) (lastValue string, result historyCheckResult) {
	history, ok := kv.lastOperation[clerkId]
	if ok {
		lastValue = history.value
		if operationId == history.operationId {
			result = lastOperation
		} else {
			result = newOperation
		}
	} else {
		result = newOperation
	}
	return
}
