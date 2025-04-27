package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
)

const (
	MAP = iota
	REDUCE
	EXIT
	WAIT
)

type Task struct {
	// 0 map task
	// 1 reduce task
	// 2 please exit
	TaskType int

	// task id.
	// The index of task in MapTasks or ReduceTasks.
	ID int

	// Input files' path.
	InputPath []string
}

const (
	MapOutputBase    string = "mr-"
	ReduceOutputBase string = "mr-out-"
)

// A simple FIFO scheduler.
type TaskScheduler struct {
	mu  sync.RWMutex
	que []*Task

	doneChan []chan struct{}
	DoneCnt  atomic.Int32
}

func (s *TaskScheduler) Init(tasks []*Task) {
	s.que = tasks
	s.doneChan = make([]chan struct{}, len(tasks))
	s.DoneCnt.Store(int32(len(tasks)))
}

func (s *TaskScheduler) Put(tasks *Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.que = append(s.que, tasks)
}

func (s *TaskScheduler) Get() *Task {
	if s.Empty() {
		return &Task{
			TaskType:  WAIT,
			ID:        -1,
			InputPath: nil,
		}
	}
	s.mu.Lock()
	task := s.que[0]
	s.que = s.que[1:]
	doneCh := make(chan struct{})
	s.doneChan[task.ID] = doneCh
	s.mu.Unlock()
	go func() {
		timer := time.NewTimer(10 * time.Second)
		select {
		case <-timer.C:
			s.Put(task)
		case <-s.doneChan[task.ID]:
			break
		}
	}()

	return task
}

func (s *TaskScheduler) Done() bool {
	return s.DoneCnt.Load() == 0
}

func (s *TaskScheduler) Empty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.que) == 0
}

func (s *TaskScheduler) SubmitTask(task *Task) {
	s.doneChan[task.ID] <- struct{}{}
	s.DoneCnt.Add(-1)
}

type Coordinator struct {
	// number of Map/Reduce tasks.
	M, R int

	sche *TaskScheduler

	inMu        []sync.Mutex
	ReduceTasks []*Task

	isDone atomic.Bool
}

// Your code here -- RPC handlers for the worker to call.

// Worker call this function to get a map or reduce task.
func (c *Coordinator) RequestTask(args *EmptyArgs, reply *AssginTaskReply) error {
	reply.Ctx = &Context{M: c.M, R: c.R}
	if c.isDone.Load() {
		reply.Task = &Task{
			TaskType:  EXIT,
			ID:        -1,
			InputPath: nil,
		}
	}
	reply.Task = c.sche.Get()
	return nil
}

// If a Worker is finish all or part of its task, it should call this function to pass back output files' location in file system.
func (c *Coordinator) SubmitLocation(args *SubmitLocationArgs, reply *EmptyReply) error {
	switch args.Task.TaskType {
	case MAP:
		if args.IsDone {
			c.sche.SubmitTask(args.Task)
			if c.sche.Done() {
				c.sche.Init(c.ReduceTasks)
			}
			break
		}
		i := len(args.FilePath) - 1
		for ; i >= 0; i-- {
			if !unicode.IsDigit(rune(args.FilePath[i])) {
				break
			}
		}
		ReduceID, _ := strconv.Atoi(args.FilePath[i+1:])
		c.inMu[ReduceID].Lock()
		c.ReduceTasks[ReduceID].InputPath = append(c.ReduceTasks[ReduceID].InputPath, args.FilePath)
		c.inMu[ReduceID].Unlock()
	case REDUCE:
		c.sche.SubmitTask(args.Task)
		if c.sche.Done() {
			c.isDone.Store(true)
		}
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
	l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.isDone.Load()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		M:           len(files),
		R:           nReduce,
		sche:        &TaskScheduler{},
		inMu:        make([]sync.Mutex, nReduce),
		ReduceTasks: make([]*Task, nReduce),
	}

	mapTasks := make([]*Task, c.M)
	for i := 0; i < c.M; i++ {
		mapTasks[i] = &Task{
			TaskType:  MAP,
			InputPath: []string{files[i]},
			ID:        i,
		}
	}

	c.sche.Init(mapTasks)

	for i := 0; i < c.R; i++ {
		c.ReduceTasks[i] = &Task{
			TaskType:  REDUCE,
			InputPath: make([]string, 0),
			ID:        i,
		}
	}

	c.server()
	// log.Printf("MapReduce Coordinator[R=%d, M=%d] is running at 127.0.0.1:1234", c.R, c.M)
	return &c
}
