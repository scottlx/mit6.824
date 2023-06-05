package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

	// enqueue task
	taskChan := make(chan *RequestTaskReply)
	go func() {
		for {
			t := AskTask()
			if t != nil {
				taskChan <- t
				//fmt.Printf("push task %v into chan\n", t)
			}
			time.Sleep(time.Second)
		}
	}()

	// process task
	for {
		select {
		case t := <-taskChan:
			//fmt.Printf("pull task %v from chan\n", t)
			switch t.Type {
			case "map":

				// 1. do mapf
				inter := LaunchMap(t.File, mapf)

				// 2. write into intermediate file
				sort.Sort(ByKey(inter))

				interFiles := make([]*os.File, t.NReduce)
				for i := 0; i < t.NReduce; i++ {
					interName := "mr-" + strconv.Itoa(t.Number) + "-" + strconv.Itoa(i)
					f, err := os.Create(interName)
					if err != nil {
						fmt.Printf("create intermediate file %s failed", interName)
						Report(TaskFailed, t.Number, "map")
						continue
					}
					interFiles[i] = f
				}

				for _, kv := range inter {
					f := interFiles[ihash(kv.Key)%t.NReduce]
					enc := json.NewEncoder(f)
					err := enc.Encode(&kv)
					if err != nil {
						fmt.Printf("write kv %v to file %s failed", kv, f.Name())
						Report(TaskFailed, t.Number, "map")
						continue
					}
				}

				for _, f := range interFiles {
					f.Close()
				}

				// 3. report task finish
				Report(TaskFinished, t.Number, "map")

			case "reduce":

				// 1. read from intermediate file
				var kva []KeyValue
				reduceNumber := t.Number - t.NMap // file suffix = reduce task id - nmap
				interFiles, err := filterDirsGlob(".", "mr-*-"+strconv.Itoa(reduceNumber))
				if err != nil {
					fmt.Printf("failed to grep intermediate file names for task %d, err: %v", t.Number, err)
				}
				for _, f := range interFiles {
					file, err := os.Open(f)
					if err != nil {
						fmt.Printf("failed to open intermediate file %s, for task %d, err: %v", f, t.Number, err)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
				}

				// 2. do reduce
				LaunchReduce(reduceNumber, kva, reducef)

				// 3. report task finish
				Report(TaskFinished, t.Number, "reduce")
			}

		}
	}

}

func filterDirsGlob(dir, suffix string) ([]string, error) {
	return filepath.Glob(filepath.Join(dir, suffix))
}

func LaunchMap(fileName string, mapf func(string, string) []KeyValue) []KeyValue {
	var intermediate []KeyValue
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	kva := mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)
	return intermediate
}

func LaunchReduce(reduceNumber int, intermediate []KeyValue, reducef func(string, []string) string) {
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(reduceNumber)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			fmt.Printf("write key: %v into %s failed", intermediate[i].Key, oname)
		}

		i = j
	}
}

func Report(status TaskStatus, taskId int, taskType string) {
	args := ReportTaskArgs{
		Status: status,
		Type:   taskType,
		TaskId: taskId,
	}
	ok := call("Coordinator.Report", &args, &ReportTaskReply{})
	if !ok {
		fmt.Printf("report task %d to coodinator failed", taskId)
	}
}

func AskTask() *RequestTaskReply {
	args := RequestTaskArgs{
		StartTime: time.Now(),
	}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		fmt.Printf("get task: %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}

	return &reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
