package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
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
	// log.Printf("report: %v", args)
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

	// use go routine to write to disk
	var wg sync.WaitGroup
	for Y := 0; Y < reply.NReduce; Y++ {
		wg.Add(1)
		go func(Y int) {
			defer wg.Done()

			oname := prefix + strconv.Itoa(Y)
			// file, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			// if err != nil {
			// 	log.Fatalf("cannot create %v", oname)
			// }
			// defer file.Close()

			// write key/value pairs in JSON format to an open file
			// enc := json.NewEncoder(file)
			// use buffer to realize atomical writing
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)

			for _, kv := range kv_buckets[Y] {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode %v", kv)
				}
			}
			atomicWriteFile(oname, &buf)
		}(Y)
	}

	wg.Wait()
	doReport(reply.Index)
}

func doReduceTask(reducef func(string, []string) string, reply GetTaskResponse) {
	Y := reply.Index // 0-9
	// log.Printf(":%v\n", Y)
	kva := []KeyValue{}

	// read all files suffixed with Y
	for X := 0; X < reply.NMap; X++ {
		iname := "mr-" + strconv.Itoa(X) + "-" + strconv.Itoa(Y)
		file, err := os.Open(iname)
		if err != nil {
			log.Fatalf("cannot open %v", iname)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva)) // maybe we can use map to avoid Sort

	// write output
	oname := "mr-out-" + strconv.Itoa(Y)
	// ofile, _ := os.Create(oname)
	var buf bytes.Buffer

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

		// fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		fmt.Fprintf(&buf, "%v %v\n", kva[i].Key, output)

		i = j
	}

	atomicWriteFile(oname, &buf)
	doReport(reply.Index)
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

func atomicWriteFile(filename string, r io.Reader) (err error) {
	// write to a temp file first, then we'll atomically replace the target file
	// with the temp file.
	dir, file := filepath.Split(filename)
	if dir == "" {
		dir = "."
	}

	f, err := ioutil.TempFile(dir, file)
	if err != nil {
		return fmt.Errorf("cannot create temp file: %v", err)
	}
	defer func() {
		if err != nil {
			// Don't leave the temp file lying around on error.
			_ = os.Remove(f.Name()) // yes, ignore the error, not much we can do about it.
		}
	}()
	// ensure we always close f. Note that this does not conflict with  the
	// close below, as close is idempotent.
	defer f.Close()
	name := f.Name()
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("cannot write data to tempfile %q: %v", name, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("can't close tempfile %q: %v", name, err)
	}

	// get the file mode from the original file and use that for the replacement
	// file, too.
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		// no original file
	} else if err != nil {
		return err
	} else {
		if err := os.Chmod(name, info.Mode()); err != nil {
			return fmt.Errorf("can't set filemode on tempfile %q: %v", name, err)
		}
	}
	if err := os.Rename(name, filename); err != nil {
		return fmt.Errorf("cannot replace %q with tempfile %q: %v", filename, name, err)
	}
	return nil
}
