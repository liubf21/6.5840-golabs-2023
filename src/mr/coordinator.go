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
	MaxTaskRunInterval = time.Second * 10
)

type SchedulePhase uint8 // record schedule phase for coordinator

const (
	MapPhase SchedulePhase = iota
	ReducePhase
	CompletePhase
)

type TaskStatus uint8 // record each task status for coordinator

const (
	Idle TaskStatus = iota
	Working
	Finished
)

type Task struct {
	id        int
	fileName  string
	startTime time.Time
	status    TaskStatus
}

type Coordinator struct {
	// Your definitions here.
	files          []string
	nReduce        int
	nMap           int
	phase          SchedulePhase
	remainingTasks []Task       // a queue of remaining tasks
	assignedTasks  map[int]Task // a map of assigned tasks

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == CompletePhase {
		reply.JobType = CompleteJob
		return nil
	}

	if len(c.remainingTasks) > 0 { // assign a task
		task := c.remainingTasks[0]
		task.status = Working
		task.startTime = time.Now()
		c.remainingTasks = c.remainingTasks[1:]
		c.assignedTasks[task.id] = task
		switch c.phase {
		case MapPhase:
			reply.JobType = MapJob
			reply.Filename = task.fileName
		case ReducePhase:
			reply.JobType = ReduceJob
		}
		reply.Index = task.id
		reply.NReduce = c.nReduce
		reply.NMap = c.nMap
	} else if len(c.assignedTasks) > 0 { // wait for a task to finish
		for _, task := range c.assignedTasks {
			if task.status == Working && time.Now().Sub(task.startTime) > MaxTaskRunInterval {
				switch c.phase {
				case MapPhase:
					reply.JobType = MapJob
					reply.Filename = task.fileName
				case ReducePhase:
					reply.JobType = ReduceJob
				}
				reply.Index = task.id
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				return nil
			}
		}
		reply.JobType = WaitJob // wait for all map tasks to finish
	} else { // all tasks finished, schedule the next phase
		if c.phase == MapPhase {
			c.phase = ReducePhase
			c.Schedule()
			reply.JobType = WaitJob // wait for next phase
		} else {
			c.phase = CompletePhase
			reply.JobType = CompleteJob
		}
	}

	// log.Printf("%v", reply)
	return nil
}

func (c *Coordinator) Report(args *ReportRequest, reply *ReportResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.assignedTasks, args.Index) // delete finished task

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false

	// Your code here.
	if c.phase == CompletePhase {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.nMap = len(files)
	c.phase = MapPhase
	c.remainingTasks = make([]Task, 0)
	c.assignedTasks = make(map[int]Task)
	c.mu = sync.Mutex{}
	c.Schedule()

	c.server()
	return &c
}

func (c *Coordinator) Schedule() {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.phase {
	case MapPhase:
		for i, file := range c.files {
			task := Task{
				id:       i,
				fileName: file,
				status:   Idle,
			}
			c.remainingTasks = append(c.remainingTasks, task)
		}
	case ReducePhase:
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				id:     i,
				status: Idle,
			}
			c.remainingTasks = append(c.remainingTasks, task)
		}
	}
}
