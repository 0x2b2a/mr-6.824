package mr

import (
	"fmt"
	"os"
	"sync"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type _Worker struct {
	mu sync.Mutex
	id int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (w *_Worker) register()  {
	request := RegisterRequest{}
	response := RegisterResponse{}

	success := call("Master.RegWorker", &request, &response)

	if ! success {
		log.Fatal("-----worker register failed-----")
	}

	w.id = response.WorkerId
}

func (w *_Worker) askTask() Task {
	request := TaskRequest{}
	response := TaskResponse{}
	request.WorkerId = w.id

	success := call("Master.AskTask", &request, &response)

	if (! success) {
		info("-----worker{%d} ask task failed-----", w.id)
		os.Exit(1)
	}

	info("-----worker{%d} get task{%+v}-----", w.id, response.Task)
	return *response.Task
}

func (w *_Worker) run() {
	for  {
		t := w.askTask()
		if ! t.alive {
			info("-----worker's task not alive-----")
			break
		}
	}
}

func (w *_Worker) doTask(t Task)  {
	info("-----worker{%id} began doing task-----", w.id)

	switch t.phase {
		case MapPhase:
			w.doMapTask(t)
		case ReducePhase:
			w.doReduceTask(t)
		default:
			panic(fmt.Sprintf("-----invalid task phase : %v-----", t.phase))
	}
}

//todo
func (w *_Worker) doMapTask(t Task) {

}

//todo
func (w *_Worker) doReduceTask(t Task) {

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
	w := _Worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.mu = sync.Mutex{}

	w.register()
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
