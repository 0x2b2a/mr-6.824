package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MaxTaskRunTime   = time.Second * 5
	ScheduleInterval = time.Millisecond * 500
)

const (
	TaskStatusReady   = 100
	TaskStatusQueue   = 101
	TaskStatusRunning = 102
	TaskStatusFinish  = 103
	TaskStatusErr     = 104
)

type Master struct {
	// Your definitions here.
	mu sync.Mutex
	tasks []_Task
	isDone bool
	nReduce int
	files []string
	taskChan chan Task
	phase int
}

type _Task struct {
	status int
	workerId int
	startTime time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AskTask(args *AskTaskRequest, response *AskTaskResponse) error {
	task := <-m.taskChan
	response.task = &task

	if task.alive {
		m.regTask()
	}

	return nil
}

func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isDone {
		return
	}

	//mapFinished := true
	//for index, t := range m.tasks {
	//	switch t.status {
	//	case :
	//	}
	//}
}

func (m *Master) tickSchedule() {
	for ! m.Done() {
		go m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)

	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.isDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.files = files

	bufferSize := max(nReduce, len(m.files))
	m.taskChan = make(chan Task, bufferSize)

	m.initMapTask()
	go m.tickSchedule()
	m.server()

	info("-----master init-----")

	return &m
}

func (m *Master) initMapTask() {
	info("-----init MapTask-----")

	m.phase = ReducePhase
	m.tasks = make([]_Task, len(m.files))
}


func (m *Master) initReduceTask() {
	info("-----init ReduceTask-----")

	m.phase = MapPhase
	m.tasks = make([]_Task, m.nReduce)
}

func (m *Master) regTask(request *AskTaskRequest, task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if task.phase != m.phase {
		panic("ask a wrong phase task")
	}

	m.tasks[task.seq].status = TaskStatusRunning
	m.tasks[task.seq].workerId = request.WorkerId
	m.tasks[task.seq].startTime = time.Now()
}