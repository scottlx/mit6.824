package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

	allTasks map[int]*Task

	mapTaskQ    chan int
	reduceTaskQ chan int

	nReduce int
	nMap    int

	mapFinished    bool
	reduceFinished bool
	mu             sync.Mutex
}

type Task struct {
	number    int
	mapFile   string
	startTime time.Time
	status    TaskStatus
	mu        sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	//fmt.Printf("got request %v\n", args)
	select {
	case taskId := <-c.mapTaskQ:
		task := c.allTasks[taskId]

		task.mu.RLock()
		reply.File = task.mapFile
		reply.Number = task.number
		task.mu.RUnlock()

		reply.Type = "map"
		reply.NReduce = c.nReduce
		reply.NMap = c.nMap
		fmt.Printf("reply: %v\n", reply)

		task.mu.Lock()
		task.status = TaskRunning
		task.startTime = args.StartTime
		task.mu.Unlock()
		return nil

	case taskId := <-c.reduceTaskQ:
		task := c.allTasks[taskId]

		task.mu.RLock()
		reply.Number = task.number
		task.mu.RUnlock()

		reply.Type = "reduce"
		reply.NReduce = c.nReduce
		reply.NMap = c.nMap
		fmt.Printf("reply: %v\n", reply)

		task.mu.Lock()
		task.status = TaskRunning
		task.startTime = args.StartTime
		task.mu.Unlock()
		return nil
	default:
		// no more tasks, all done ^.^
		fmt.Println("no tasks available")
		return nil
	}
}

func (c *Coordinator) Report(args *ReportTaskArgs, reply *ReportTaskReply) error {
	//fmt.Printf("got request %v\n", args)
	t := c.allTasks[args.TaskId]
	t.mu.Lock()
	t.status = args.Status
	if args.Status != TaskFinished {
		switch args.Type {
		case "map":
			c.mapTaskQ <- t.number
		case "reduce":
			c.reduceTaskQ <- t.number
		}
	}
	t.mu.Unlock()

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	res := c.mapFinished && c.reduceFinished
	c.mu.Unlock()
	return res
}

func (c *Coordinator) daemon() {

	go func() {
		for {
			var finishedMaps, finishedReduces int

			for _, task := range c.allTasks {
				// check died task
				task.mu.Lock()
				if (task.status == TaskRunning || task.status == TaskFailed) &&
					time.Now().After(task.startTime.Add(10*time.Second)) {
					fmt.Printf("timeout task %d\n", task.number)
					if task.number >= c.nMap {
						fmt.Printf("pushed task %d into reduceTaskQ\n", task.number)
						c.reduceTaskQ <- task.number
					} else {
						fmt.Printf("pushed task %d into mapTaskQ\n", task.number)
						c.mapTaskQ <- task.number
					}
					task.status = TaskFailed
				}

				// check global jobs finish status
				if task.status == TaskFinished {
					if task.number < c.nMap {
						finishedMaps++
					} else {
						finishedReduces++
					}
				}
				task.mu.Unlock()
			}

			fmt.Printf("finishedMaps:%d, finishedReduces:%d\n", finishedMaps, finishedReduces)

			if !c.mapFinished && finishedMaps == c.nMap {
				// start reduce jobs
				for i := 0; i < c.nReduce; i++ {
					c.reduceTaskQ <- i + c.nMap
				}
				c.mu.Lock()
				c.mapFinished = true
				c.mu.Unlock()
			}
			if finishedReduces == c.nReduce {
				c.mu.Lock()
				c.reduceFinished = true
				c.mu.Unlock()
			}

			time.Sleep(time.Second)
		}
	}()

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMap = len(files)
	c.nReduce = nReduce
	c.allTasks = make(map[int]*Task)
	c.mapTaskQ = make(chan int, c.nMap)
	c.reduceTaskQ = make(chan int, c.nReduce)

	for i := 0; i < c.nMap; i++ {
		c.allTasks[i] = &Task{
			number:  i,
			mapFile: files[i],
			status:  TaskInit,
			mu:      sync.RWMutex{},
		}
		// init map task q
		c.mapTaskQ <- i
	}

	// reduce task id starts with c.nMap
	for i := 0; i < c.nReduce; i++ {
		c.allTasks[i+c.nMap] = &Task{
			number: i + c.nMap,
			status: TaskInit,
			mu:     sync.RWMutex{},
		}
	}

	c.server()
	c.daemon()
	return &c
}
