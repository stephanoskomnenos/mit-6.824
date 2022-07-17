package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	lastTask := Task{Type: Empty}
	newTask := Task{Type: Empty}

	for {
		ok := call("Coordinator.AssignTask", &lastTask, &newTask)
		if !ok {
			fmt.Println("call failed!")
			newTask.Type = Empty
		}

		if newTask.Type == Empty {
			time.Sleep(time.Second)
		} else if newTask.Type == Map {
			mapTask(mapf, &newTask)
		} else if newTask.Type == Reduce {
			reduceTask(reducef, &newTask)
		}

		lastTask = newTask
		newTask = Task{Type: Empty}
	}
}

func mapTask(mapf func(string, string) []KeyValue, task *Task) {
	task.Status = Running

	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
		task.Status = Failed
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
		task.Status = Failed
		return
	}
	file.Close()
	kva := mapf(task.FileName, string(content))

	tmpFilePrefix := fmt.Sprintf("mr-tmp-%d-", task.Id)
	tmpFiles := []*os.File{}
	encs := []*json.Encoder{}

	for id := 0; id < task.NReduce; id++ {
		tmpFileName := tmpFilePrefix + fmt.Sprint(id)
		tmpFile, err := os.Create(tmpFileName)
		if err != nil {
			log.Fatalf("cannot open %s", tmpFileName)
			task.Status = Failed
			return
		}
		enc := json.NewEncoder(tmpFile)
		tmpFiles = append(tmpFiles, tmpFile)
		encs = append(encs, enc)
	}

	for _, kv := range kva {
		err := encs[ihash(kv.Key)%task.NReduce].Encode(kv)
		if err != nil {
			log.Fatalf("error encoding %s", kv)
			task.Status = Failed
			return
		}
	}

	for _, file := range tmpFiles {
		file.Close()
	}

	task.Status = Success
}

func reduceTask(reducef func(string, []string) string, task *Task) {
	task.Status = Running

	intermediate := []KeyValue{}

	for mapNum := 0; mapNum < task.NMap; mapNum++ {
		tmpFileName := fmt.Sprintf("mr-tmp-%d-%d", mapNum, task.Id)

		tmpFile, err := os.Open(tmpFileName)
		if err != nil {
			log.Fatalf("cannot open %s", tmpFileName)
			task.Status = Failed
			return
		}

		dec := json.NewDecoder(tmpFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		tmpFile.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", task.Id)
	ofile, _ := os.Create(oname)

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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	task.Status = Success
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
