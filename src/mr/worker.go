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
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		// ask for a task
		// args := HeartbeatRequest{}
		// reply := HeartbeatResponse{}
		// ok := call("Coordinator.GetTask", &args, &reply)

		// if !ok {
		// 	return // maybe the coordinator has exited
		// }
		reply := doHeartbeat()
		log.Printf("Worker: receive coordinator's heartbeat %v \n", reply)

		switch reply.JobType {
		case MapJob: // Map
			doMapTask(mapf, reply)
		case ReduceJob: // Reduce
			doReduceTask(reducef, reply)
		case WaitJob:
			time.Sleep(1 * time.Second)
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("unexpected JobType %v", reply.JobType))
		}

		doReport(reply.Index)
	}
}

func doHeartbeat() GetTaskResponse {
	args := GetTaskRequest{}
	reply := GetTaskResponse{}
	call("Coordinator.GetTask", &args, &reply)

	return reply
}

func doReport(i int) {
	args := ReportRequest{Index: i}
	reply := ReportResponse{}
	call("Coordinator.Report", &args, &reply)
}

func doMapTask(mapf func(string, string) []KeyValue, reply GetTaskResponse) {

	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()
	kva := mapf(reply.Filename, string(content))

	// divide
	kv_buckets := make([][]KeyValue, 10)
	for _, kv := range kva {
		Y := ihash(kv.Key) % reply.NReduce // buckets
		kv_buckets[Y] = append(kv_buckets[Y], kv)
	}

	// write intermediate data to disk
	X := reply.Index
	prefix := "mr-" + strconv.Itoa(X) + "-"

	for Y := 0; Y < reply.NReduce; Y++ {
		oname := prefix + strconv.Itoa(Y)

		file, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		}
		defer file.Close()

		// write key/value pairs in JSON format to an open file
		enc := json.NewEncoder(file)
		for _, kv := range kv_buckets[Y] {
			enc.Encode(&kv)
		}
	}
}

func doReduceTask(reducef func(string, []string) string, reply GetTaskResponse) {
	Y := reply.Index // 0-9
	// log.Printf(":%v\n", Y)
	kva := []KeyValue{}

	// read all files suffixed with Y
	for X := 0; X < reply.NMap; X++ {
		iname := "mr-" + strconv.Itoa(X) + "-" + strconv.Itoa(Y)
		file, _ := os.Open(iname)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	// write output
	oname := "mr-out-" + strconv.Itoa(Y)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
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
