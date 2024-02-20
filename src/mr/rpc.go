package mr

import "os"
import "strconv"

type GetTaskArgs struct {
	Nothing int
}
type GetTaskReply struct {
	Task MrTask
}

type ReportTaskArgs struct {
	Task MrTask
}
type ReportTaskReply struct {
	Nothing int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
