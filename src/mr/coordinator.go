package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.

	MapTasks    map[int]Task
	ReduceTasks map[int]Task
	NReduce     int
	NMap        int

	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *Task, reply *Task) error {
	c.mutex.Lock()

	if args.Type == Map && args.Status != Success {
		mapTask := c.MapTasks[args.Id]
		mapTask.Status = Idle
		c.MapTasks[args.Id] = mapTask
	} else if args.Type == Map && args.Status == Success {
		delete(c.MapTasks, args.Id)
	}

	if args.Type == Reduce && args.Status != Success {
		reduceTask := c.ReduceTasks[args.Id]
		reduceTask.Status = Idle
		c.ReduceTasks[args.Id] = reduceTask
	} else if args.Type == Reduce && args.Status == Success {
		delete(c.ReduceTasks, args.Id)
	}

	reply.Type = Empty

	if len(c.MapTasks) != 0 {
		for id, mapTask := range c.MapTasks {
			if mapTask.Status == Running {
				continue
			}
			reply.Type = Map
			reply.Id = id
			reply.FileName = mapTask.FileName
			reply.NReduce = c.NReduce
			mapTask.Status = Running
			break
		}
	} else if len(c.ReduceTasks) != 0 {
		for id, reduceTask := range c.ReduceTasks {
			if reduceTask.Status == Running {
				continue
			}
			reply.Type = Reduce
			reply.Id = id
			reply.NMap = c.NMap
			reply.NReduce = c.NReduce
			reduceTask.Status = Running
			break
		}
	}

	c.mutex.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mutex.Lock()

	ret = len(c.MapTasks) == 0 && len(c.ReduceTasks) == 0

	c.mutex.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.MapTasks = make(map[int]Task)
	c.ReduceTasks = make(map[int]Task)
	c.NReduce = nReduce

	// generate map tasks
	for i, file := range files {
		mapTask := Task{Id: i, Type: Map, FileName: file, Status: Idle, NReduce: c.NReduce}
		c.MapTasks[i] = mapTask
	}
	c.NMap = len(c.MapTasks)

	// generate reduce tasks
	for id := 0; id < nReduce; id++ {
		reduceTask := Task{Id: id, Type: Reduce, Status: Idle, NReduce: c.NReduce}
		c.ReduceTasks[id] = reduceTask
	}

	c.server()
	return &c
}
