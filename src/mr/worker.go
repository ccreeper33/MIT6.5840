package mr

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getTaskCall() MrTask {

	args := GetTaskArgs{}
	reply := GetTaskReply{}

	for {
		ok := call("Coordinator.GetTask", &args, &reply)
		if ok {
			return reply.Task
		} else {
			log.Fatalf("Get Task failed!\n")
		}
	}
}

func ReportTaskCall(task MrTask) {

	args := ReportTaskArgs{}
	args.Task = task
	reply := ReportTaskReply{}

	for {
		ok := call("Coordinator.ReportTask", &args, &reply)
		if ok {
			return
		} else {
			log.Fatalf("Report Task failed!\n")
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapFunc func(string, string) []KeyValue,
	reduceFunc func(string, []string) string) {
	for {
		task := getTaskCall()
		if task.Type == Map {
			// read from input file
			file, err := os.Open(task.InputFileName)
			if err != nil {
				log.Fatalf("cannot open input file %v", task.InputFileName)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.InputFileName)
			}
			err = file.Close()
			if err != nil {
				log.Fatalf("cannot close %v: %v", task.InputFileName, err)
			}

			// run map function
			keyValues := mapFunc(task.InputFileName, string(content))

			// split intermediate data into R(or NReduce) pieces
			interDatas := make([][]KeyValue, 0)
			for i := 0; i < task.NReduce; i++ {
				interDatas = append(interDatas, make([]KeyValue, 0))
			}
			for _, kv := range keyValues {
				index := ihash(kv.Key) % task.NReduce
				interDatas[index] = append(interDatas[index], kv)
			}

			// serialize and write intermediate file
			for i := 0; i < task.NReduce; i++ {
				var buffer bytes.Buffer
				enc := gob.NewEncoder(&buffer)
				err := enc.Encode(interDatas[i])
				if err != nil {
					log.Fatal("encode error:", err)
				}
				interFileName := fmt.Sprintf("mr-map-%v-reduce-%v-%v", task.Id, i, task.CreateTime)
				file, err := os.Create(interFileName)
				if err != nil {
					log.Fatalf("cannot create %v", interFileName)
				}
				_, err = file.Write(buffer.Bytes())
				if err != nil {
					log.Fatalf("cannot write %v", interFileName)
				}
				err = file.Close()
				if err != nil {
					log.Fatalf("cannot close %v", interFileName)
				}
				task.InterFileNames = append(task.InterFileNames, interFileName)
			}
			task.Status = Completed

		} else if task.Type == Reduce {
			// read and deserialize intermedia data
			var interData []KeyValue
			interNum := len(task.InterFileNames)
			for i := 0; i < interNum; i++ {
				var buffer bytes.Buffer
				dec := gob.NewDecoder(&buffer)
				interFileName := task.InterFileNames[i]
				file, err := os.Open(interFileName)
				if err != nil {
					log.Fatalf("cannot open intermediate file %v", interFileName)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", interFileName)
				}
				err = file.Close()
				if err != nil {
					log.Fatalf("cannot close %v", interFileName)
				}
				buffer.Write(content)
				var data []KeyValue
				err = dec.Decode(&data)
				if err != nil {
					log.Fatal("decode error:", err)
				}
				interData = append(interData, data...)
			}
			// prepare (Key, list(Value)) for reduce function
			kvMap := make(map[string][]string)
			for _, kv := range interData {
				elem, ok := kvMap[kv.Key]
				if ok {
					kvMap[kv.Key] = append(elem, kv.Value)
				} else {
					kvMap[kv.Key] = []string{kv.Value}
				}
			}

			// run reduce function
			outputString := ""
			for key, values := range kvMap {
				outputString += fmt.Sprintf("%v %v\n", key, reduceFunc(key, values))
			}

			// write output file
			outputFileName := fmt.Sprintf("mr-out-%v-%v", task.Id, task.CreateTime)
			file, err := os.Create(outputFileName)
			if err != nil {
				log.Fatalf("cannot create %v", outputFileName)
			}
			_, err = file.WriteString(outputString)
			if err != nil {
				log.Fatalf("cannot write %v", outputFileName)
			}
			err = file.Close()
			if err != nil {
				log.Fatalf("cannot close %v", outputFileName)
			}
			task.OutputFileName = outputFileName
			task.Status = Completed

		} else if task.Type == NoMoreTasks {
			os.Exit(0)
		}

		// report Task
		ReportTaskCall(task)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcName string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer func(c *rpc.Client) {
		err := c.Close()
		if err != nil {
			log.Fatal("error closing rpc client:", err)
		}
	}(c)

	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
