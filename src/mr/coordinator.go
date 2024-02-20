package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	inputFileNames []string
	nReduce        int              // number of reduce tasks
	workerTimeOut  time.Duration    // time to re-assign tasks, default 10s
	state          coordinatorState // coordinator state
	tasks          [][]MrTask       // mapreduce tasks, example: task[Map][taskId]
	taskCount      int              // the number of tasks which are not completed
	taskLock       sync.Mutex       // lock for update Task Status
	taskChannel    chan MrTask      // channel to assign Idle tasks
}

func (c *Coordinator) generateMapTasks() {
	taskNumber := len(c.inputFileNames)
	for i := 0; i < taskNumber; i++ {
		task := MrTask{
			Id:             i,
			CreateTime:     time.Now().Unix(),
			NReduce:        c.nReduce,
			Type:           Map,
			Status:         Idle,
			InputFileName:  c.inputFileNames[i],
			InterFileNames: make([]string, 0),
		}
		c.tasks[Map] = append(c.tasks[Map], task)
	}
	c.taskCount = taskNumber
	c.state = mapping
	for _, task := range c.tasks[Map] {
		c.taskChannel <- task
	}
}

func (c *Coordinator) generateReduceTasks() {
	mapTaskNumber := len(c.inputFileNames)
	reduceTaskNumber := c.nReduce

	for i := 0; i < reduceTaskNumber; i++ {
		task := MrTask{
			Id:             i,
			CreateTime:     time.Now().Unix(),
			NReduce:        c.nReduce,
			Type:           Reduce,
			Status:         Idle,
			InterFileNames: make([]string, 0),
		}
		c.tasks[Reduce] = append(c.tasks[Reduce], task)
	}

	// shuffle intermediate files
	for mapIndex := 0; mapIndex < mapTaskNumber; mapIndex++ {
		for reduceIndex := 0; reduceIndex < reduceTaskNumber; reduceIndex++ {
			c.tasks[Reduce][reduceIndex].InterFileNames =
				append(c.tasks[Reduce][reduceIndex].InterFileNames,
					c.tasks[Map][mapIndex].InterFileNames[reduceIndex])
		}
	}

	c.taskCount = reduceTaskNumber
	c.state = reducing
	for _, task := range c.tasks[Reduce] {
		c.taskChannel <- task
	}
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	_ = args
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	if c.state == done {
		reply.Task = MrTask{Type: NoMoreTasks}
	} else {
		task := <-c.taskChannel
		c.tasks[task.Type][task.Id].Status = InProgress
		go c.taskTimeOut(&c.tasks[task.Type][task.Id])
		reply.Task = task
	}
	return nil
}

func (c *Coordinator) taskTimeOut(task *MrTask) {
	time.Sleep(c.workerTimeOut)
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	if task.Status != Completed {
		// worker time out, create a new task
		task.Status = Idle
		task.CreateTime = time.Now().Unix()
		c.taskChannel <- *task
	}
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	_ = reply
	taskId := args.Task.Id
	taskType := args.Task.Type
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	if c.tasks[taskType][taskId].CreateTime == args.Task.CreateTime {
		// completed in time, take as valid report
		c.tasks[taskType][taskId] = args.Task
		c.taskCount--
		if c.taskCount == 0 {
			if c.state == mapping {
				c.generateReduceTasks()
				c.state = reducing
			} else if c.state == reducing {
				c.CleanUp()
				c.state = done
			}
		}
	} else {
		// time out, invalid report
		if args.Task.Type == Map {
			deleteFiles(args.Task.InterFileNames)
		} else {
			deleteFiles([]string{args.Task.OutputFileName})
		}
	}
	return nil
}

// CleanUp - remove intermedia files & rename output files
func (c *Coordinator) CleanUp() {
	for _, task := range c.tasks[Reduce] {
		deleteFiles(task.InterFileNames)
		renameFile(task.OutputFileName, fmt.Sprintf("mr-out-%v", task.Id))
	}
}

func deleteFiles(fileNames []string) {
	for _, fileName := range fileNames {
		err := os.Remove(fileName)
		if err != nil {
			log.Printf("cannot remove %v\n", fileName)
		}
	}
}

func renameFile(old, new string) {
	err := os.Rename(old, new)
	if err != nil {
		log.Printf("cannot rename %v to %v\n", old, new)
	}
}

type coordinatorState int

const (
	creating coordinatorState = iota
	mapping
	reducing
	done
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
	NoMoreTasks
)

type MrTaskStatus int

const (
	Idle MrTaskStatus = iota
	InProgress
	Completed
)

type MrTask struct {
	Id             int
	CreateTime     int64
	NReduce        int
	Type           TaskType
	Status         MrTaskStatus
	InputFileName  string
	InterFileNames []string
	OutputFileName string
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		log.Fatal("rpc register error:", err)
	}
	rpc.HandleHTTP()
	//listener, err := net.Listen("tcp", ":1234")
	sockName := coordinatorSock()
	_ = os.Remove(sockName) // remove old socket
	listener, err := net.Listen("unix", sockName)
	if err != nil {
		log.Fatal("listen error: ", err)
	}
	go func() {
		err := http.Serve(listener, nil)
		if err != nil {
			log.Fatal("serve error: ", err)
		}
	}()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.state == done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inputFileNames: files,
		nReduce:        nReduce,
		workerTimeOut:  time.Second * 10,
		state:          creating,
		tasks:          [][]MrTask{make([]MrTask, 0), make([]MrTask, 0)},
		taskLock:       sync.Mutex{},
		taskChannel:    make(chan MrTask, max_(len(files), nReduce)+1),
	}
	c.taskLock.Lock()
	c.generateMapTasks()
	c.taskLock.Unlock()
	c.server()
	return &c
}

// do not exist on Go 1.15
func max_(x, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}
