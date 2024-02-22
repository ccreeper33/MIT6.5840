package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	server      *labrpc.ClientEnd
	id          int64
	operationId int64
}

func nRand() int64 {
	max_ := big.NewInt(int64(1) << 62)
	bigX, _ := rand.Int(rand.Reader, max_)
	x := bigX.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.id = nRand()
	ck.operationId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (value string) {
	for done := false; !done; {
		args := GetArgs{}
		args.Key = key
		args.ClerkId = ck.id
		args.OperationId = ck.operationId
		reply := GetReply{}
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			value = reply.Value
			ck.operationId++
			done = true
		}
	}
	return
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) (oldValue string) {
	for done := false; !done; {
		args := PutAppendArgs{}
		args.Key = key
		args.Value = value
		args.ClerkId = ck.id
		args.OperationId = ck.operationId
		reply := PutAppendReply{}
		ok := ck.server.Call("KVServer."+op, &args, &reply)
		if ok {
			oldValue = reply.Value
			ck.operationId++
			done = true
		}
	}
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
