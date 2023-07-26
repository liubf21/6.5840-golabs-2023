package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Coordinator struct {
	// Your definitions here.
	files       []string
	nReduce     int
	startReduce int
	doneReduce  int
	nMap        int
	startMap    int
	doneMap     int
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *MyArgs, reply *MyReply) error {
	go func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.startMap < c.nMap { // assign map task
			reply.JobType = MapJob
			reply.Index = c.startMap
			reply.Filename = c.files[c.startMap]
			c.startMap++
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
		} else if c.doneMap < c.nMap {
			reply.JobType = WaitJob
		} else if c.startReduce < c.nReduce {
			reply.JobType = ReduceJob
			reply.Index = c.startReduce
			c.startReduce++
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
		} else if c.doneReduce < c.nReduce {
			reply.JobType = WaitJob
		} else {
			reply.JobType = CompleteJob
		}
	}()
	// log.Printf("%v", reply)
	return nil
}

func (c *Coordinator) DoneTask(args *MyReply, reply *MyArgs) error {
	if args.JobType == MapJob {
		c.doneMap++
	} else if args.JobType == ReduceJob {
		c.doneReduce++
	}
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
	if c.doneReduce == c.nReduce {
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
	c.startReduce = 0
	c.doneReduce = 0
	c.nMap = len(files)
	c.startMap = 0
	c.doneMap = 0

	c.server()
	return &c
}
