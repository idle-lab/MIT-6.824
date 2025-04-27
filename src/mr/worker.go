package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) string

// var types = []string{"MAP", "REDUCE", "EXIT", "WAIT"}

// main/mrworker.go calls this function.
func Worker(mapf MapFunc, reducef ReduceFunc) {
	// Your worker implementation here.
	for {
		reply := AssginTaskReply{}

		ok := call("Coordinator.RequestTask", &EmptyArgs{}, &reply)
		if !ok {
			log.Printf("coordinator done.")
			return
		}

		// log.Printf("Get a Task[Type=%s, ID=%d]", types[reply.Task.TaskType], reply.Task.ID)

		switch reply.Task.TaskType {
		case MAP:
			if err := runMap(reply.Ctx, mapf, reply.Task); err != nil {
				panic(err)
			}
		case REDUCE:
			if err := runReduce(reply.Ctx, reducef, reply.Task); err != nil {
				panic(err)
			}
		case EXIT:
			return
		case WAIT:
			time.Sleep(time.Second)
		}
	}
}

func runMap(ctx *Context, fun MapFunc, task *Task) error {
	inf, err := os.Open(task.InputPath[0])
	if err != nil {
		return err
	}
	defer inf.Close()
	content, err := io.ReadAll(inf)
	if err != nil {
		return err
	}

	kvs := fun(task.InputPath[0], string(content))
	mp := make(map[int][]KeyValue)
	for i := 0; i < len(kvs); i++ {
		reduceId := ihash(kvs[i].Key) % ctx.R
		if _, ok := mp[reduceId]; !ok {
			mp[reduceId] = make([]KeyValue, 0)
		}
		mp[reduceId] = append(mp[reduceId], kvs[i])
	}

	var wg sync.WaitGroup
	for reduceId, kvs := range mp {
		// write to local disk
		wg.Add(1)
		go func() {
			outputFile := MapOutputBase + strconv.Itoa(task.ID) + "-" + strconv.Itoa(reduceId)
			f, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				panic(err)
			}
			defer f.Close()
			buffer := bufio.NewWriter(f)
			defer buffer.Flush()

			enc := json.NewEncoder(buffer)
			for _, kv := range kvs {
				err := enc.Encode(kv)
				if err != nil {
					log.Printf("encode kv pair [%s, %s] failed [%s]", kv.Key, kv.Value, err)
					panic(err)
				}
			}

			ok := call("Coordinator.SubmitLocation", &SubmitLocationArgs{
				Task:     task,
				FilePath: outputFile,
				IsDone:   false,
			}, &EmptyReply{})
			if !ok {
				panic(fmt.Sprintf("mr%d-%d submit file location falied.", task.ID, reduceId))
			}
			wg.Done()
		}()
	}

	wg.Wait()
	ok := call("Coordinator.SubmitLocation", &SubmitLocationArgs{
		Task:     task,
		FilePath: "",
		IsDone:   true,
	}, &EmptyReply{})
	if !ok {
		panic("submit finish signal falied")
	}
	return nil
}

func runReduce(ctx *Context, fun ReduceFunc, task *Task) error {
	kvs := make([]KeyValue, 0)

	for _, input := range task.InputPath {
		f, err := os.Open(input)
		if err != nil {
			return err
		}
		defer f.Close()
		dec := json.NewDecoder(f)
		for i := 0; ; i++ {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}

	fileName := ReduceOutputBase + strconv.Itoa(task.ID)
	f, err := os.CreateTemp("", fileName)
	if err != nil {
		log.Printf("create temp file %s failed", fileName)
		return err
	}
	defer f.Close()
	buffer := bufio.NewWriter(f)
	defer buffer.Flush()

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	i := 0
	for i < len(kvs) {
		j := i + 1
		for ; j < len(kvs) && kvs[j].Key == kvs[i].Key; j++ {
		}

		values := make([]string, j-i)
		for k := 0; k < j-i; k++ {
			values[k] = kvs[i+k].Value
		}

		res := fun(kvs[i].Key, values)
		fmt.Fprintf(buffer, "%v %v\n", kvs[i].Key, res)

		i = j
	}
	os.Rename(f.Name(), fileName)

	ok := call("Coordinator.SubmitLocation", &SubmitLocationArgs{
		Task:     task,
		FilePath: fileName,
		IsDone:   false,
	}, &EmptyReply{})

	if !ok {
		panic("Reduce call Coordinator.SubmitLocation failed")
	}

	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
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
