package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key         string
	Value       string
	ClerkId     int64
	OperationId int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key         string
	ClerkId     int64
	OperationId int64
}

type GetReply struct {
	Value string
}
