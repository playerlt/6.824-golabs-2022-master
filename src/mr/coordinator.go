package mr

import (
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const maxTaskTime = 10 // seconds

type MapTaskState struct {
	beginSecond int64
	workerId    int
	fileId      int
}

type ReduceTaskState struct {
	beginSecond int64
	workerId    int
	fileId      int
}

type Coordinator struct {
	// Your definitions here.
	fileNames []string
	nReduce   int

	curWorkerId int

	unIssuedMapTasks chan int
	issuedMapTasks   map[interface{}]bool
	issuedMapMutex   sync.Mutex

	unIssuedReduceTasks chan int
	issuedReduceTasks   map[interface{}]bool
	issuedReduceMutex   sync.Mutex

	// task states
	mapTasks    []MapTaskState
	reduceTasks []ReduceTaskState

	// states
	mapDone bool
	allDone bool
}

// Your code here -- RPC handlers for the worker to call.

type MapTaskArgs struct {
	// -1 if does not have one
	WorkerId int
}
type MapTaskReply struct {
	// worker passes this to the os package
	FileName string

	// marks a unique file for mapping
	// gives -1 for no more fileId
	FileId int

	// for reduce tasks
	NReduce int

	// assign worker id as this reply is the first sent to workers
	WorkerId int

	// whether this kind of tasks are all done
	// if not, and fileId is -1, the worker waits
	AllDone bool
}

func mapDoneProcess(reply *MapTaskReply) {
	log.Println("all map tasks complete, telling workers to switch to reduce mode")
	reply.FileId = -1
	reply.AllDone = true
}

func (c *Coordinator) GiveMapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	c.issuedMapMutex.Lock()
	defer c.issuedMapMutex.Unlock()
	if args.WorkerId == -1 {
		// simply allocate
		reply.WorkerId = c.curWorkerId
		c.curWorkerId++
	} else {
		reply.WorkerId = args.WorkerId
	}
	log.Printf("worker %v asks for a map task\n", reply.WorkerId)

	if c.mapDone {
		mapDoneProcess(reply)
		return nil
	}

	if len(c.unIssuedMapTasks) == 0 && len(c.issuedMapTasks) == 0 {
		mapDoneProcess(reply)
		c.prepareAllReduceTasks()
		c.mapDone = true
		return nil
	}

	log.Printf("%v unissued map tasks %v issued map tasks at hand\n", len(c.unIssuedMapTasks), len(c.issuedMapTasks))

	curTime := getNowTimeSecond()
	l := len(c.unIssuedMapTasks)
	var fileId int
	if l == 0 {
		log.Println("no map task yet, let worker wait...")
		fileId = -1
	} else {
		fileId = <-c.unIssuedMapTasks
		reply.FileName = c.fileNames[fileId]
		c.mapTasks[fileId].beginSecond = curTime
		c.mapTasks[fileId].workerId = reply.WorkerId
		c.issuedMapTasks[fileId] = true
		log.Printf("giving map task %v on file %v at second %v\n", fileId, reply.FileName, curTime)
	}

	reply.FileId = fileId
	reply.AllDone = false
	reply.NReduce = c.nReduce

	return nil
}

type MapTaskJoinArgs struct {
	FileId   int
	WorkerId int
}

type MapTaskJoinReply struct {
	Accept bool
}

func getNowTimeSecond() int64 {
	return time.Now().UnixNano() / int64(time.Second)
}

func (c *Coordinator) JoinMapTask(args *MapTaskJoinArgs, reply *MapTaskJoinReply) error {
	// check current time for whether the worker has timed out
	log.Printf("got join request from worker %v on file %v %v\n", args.WorkerId, args.FileId, c.fileNames[args.FileId])

	curTime := getNowTimeSecond()
	taskTime := c.mapTasks[args.FileId].beginSecond
	c.issuedMapMutex.Lock()
	defer c.issuedMapMutex.Unlock()

	if !c.issuedMapTasks[args.FileId] {
		log.Println("task abandoned or does not exists, ignoring...")
		reply.Accept = false
		return nil
	}
	if c.mapTasks[args.FileId].workerId != args.WorkerId {
		log.Printf("map task belongs to worker %v not this %v, ignoring...", c.mapTasks[args.FileId].workerId, args.WorkerId)
		reply.Accept = false
		return nil
	}
	if curTime-taskTime > maxTaskTime {
		log.Println("task exceeds max wait time, abadoning...")
		reply.Accept = false
		//c.unIssuedMapTasks <- args.FileId
	} else {
		log.Println("task within max wait time, accepting...")
		reply.Accept = true
		delete(c.issuedMapTasks, args.FileId)
	}
	return nil
}

type ReduceTaskArgs struct {
	WorkerId int
}

type ReduceTaskReply struct {
	RIndex    int
	NReduce   int
	FileCount int
	AllDone   bool
}

func (c *Coordinator) prepareAllReduceTasks() {
	for i := 0; i < c.nReduce; i++ {
		log.Printf("putting %vth reduce task into channel\n", i)
		c.unIssuedReduceTasks <- i
	}
}

func (c *Coordinator) GiveReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	log.Printf("worker %v asking for a reduce task\n", args.WorkerId)
	c.issuedReduceMutex.Lock()
	defer c.issuedReduceMutex.Unlock()

	if len(c.unIssuedReduceTasks) == 0 && len(c.issuedReduceTasks) == 0 {
		log.Println("all reduce tasks complete, telling workers to terminate")
		c.allDone = true
		reply.RIndex = -1
		reply.AllDone = true
		return nil
	}
	log.Printf("%v unissued reduce tasks %v issued reduce tasks at hand\n", len(c.unIssuedReduceTasks), len(c.issuedReduceTasks))
	curTime := getNowTimeSecond()
	l := len(c.unIssuedReduceTasks)
	var rindex int
	if l == 0 {
		log.Println("no reduce task yet, let worker wait...")
		rindex = -1
	} else {
		rindex = <-c.unIssuedReduceTasks
		c.reduceTasks[rindex].beginSecond = curTime
		c.reduceTasks[rindex].workerId = args.WorkerId
		c.issuedReduceTasks[rindex] = true
		log.Printf("giving reduce task %v at second %v\n", rindex, curTime)
	}

	reply.RIndex = rindex
	reply.AllDone = false
	reply.NReduce = c.nReduce
	reply.FileCount = len(c.fileNames)

	return nil
}

type ReduceTaskJoinArgs struct {
	WorkerId int
	RIndex   int
}

type ReduceTaskJoinReply struct {
	Accept bool
}

func (c *Coordinator) JoinReduceTask(args *ReduceTaskJoinArgs, reply *ReduceTaskJoinReply) error {
	// check current time for whether the worker has timed out
	log.Printf("got join request from worker %v on reduce task %v\n", args.WorkerId, args.RIndex)

	curTime := getNowTimeSecond()
	taskTime := c.reduceTasks[args.RIndex].beginSecond
	if !c.issuedReduceTasks[args.RIndex] {
		log.Println("task abandoned or does not exists, ignoring...")
		return nil
	}
	if c.reduceTasks[args.RIndex].workerId != args.WorkerId {
		log.Printf("reduce task belongs to worker %v not this %v, ignoring...", c.reduceTasks[args.RIndex].workerId, args.WorkerId)
		reply.Accept = false
		return nil
	}
	if curTime-taskTime > maxTaskTime {
		log.Println("task exceeds max wait time, abadoning...")
		reply.Accept = false
	} else {
		log.Println("task within max wait time, accepting...")
		reply.Accept = true
		c.issuedReduceMutex.Lock()
		delete(c.issuedReduceTasks, args.RIndex)
		c.issuedReduceMutex.Unlock()
	}

	return nil
}

func removeTimeoutMapTasks(mapTasks []MapTaskState, m map[interface{}]bool, unIssuedMapTasks chan int) {
	for fileId, issued := range m {
		now := getNowTimeSecond()
		if issued {
			if now-mapTasks[fileId.(int)].beginSecond > maxTaskTime {
				log.Printf("worker %v on file %v abandoned due to timeout\n", mapTasks[fileId.(int)].workerId, fileId)
				delete(m, fileId)
				unIssuedMapTasks <- fileId.(int)
			}
		}
	}
}

func removeTimeoutReduceTasks(reduceTasks []ReduceTaskState, m map[interface{}]bool, unIssuedReduceTasks chan int) {
	for fileId, issued := range m {
		now := getNowTimeSecond()
		if issued {
			if now-reduceTasks[fileId.(int)].beginSecond > maxTaskTime {
				log.Printf("worker %v on file %v abandoned due to timeout\n", reduceTasks[fileId.(int)].workerId, fileId)
				delete(m, fileId)
				unIssuedReduceTasks <- fileId.(int)
			}
		}
	}
}

func (c *Coordinator) removeTimeoutTasks() {
	log.Println("removing timeout maptasks...")
	c.issuedMapMutex.Lock()
	removeTimeoutMapTasks(c.mapTasks, c.issuedMapTasks, c.unIssuedMapTasks)
	c.issuedMapMutex.Unlock()
	c.issuedReduceMutex.Lock()
	removeTimeoutReduceTasks(c.reduceTasks, c.issuedReduceTasks, c.unIssuedReduceTasks)
	c.issuedReduceMutex.Unlock()
}

func (c *Coordinator) loopRemoveTimeoutMapTasks() {
	for true {
		time.Sleep(maxTaskTime * time.Second)
		c.removeTimeoutTasks()
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	// log.Println("111...")
	rpc.Register(c)
	// log.Println("222...")
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	// log.Println("333...")
	sockname := coordinatorSock()
	// log.Println("444...")
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	// log.Println("listen started...")
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// ret := false

	// Your code here.

	// return ret

	if c.allDone {
		log.Println("asked whether i am done, replying yes...")
	} else {
		log.Println("asked whether i am done, replying no...")
	}

	return c.allDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	log.SetFlags(0)
	log.SetOutput(io.Discard)
	log.SetPrefix("coordinator: ")
	log.Println("making coordinator")

	c.fileNames = files
	c.nReduce = nReduce
	c.curWorkerId = 0
	c.mapTasks = make([]MapTaskState, len(files))
	c.reduceTasks = make([]ReduceTaskState, nReduce)
	c.unIssuedMapTasks = make(chan int, len(files))
	c.issuedMapTasks = make(map[interface{}]bool)
	c.issuedReduceTasks = make(map[interface{}]bool)
	c.unIssuedReduceTasks = make(chan int, nReduce)
	c.allDone = false
	c.mapDone = false

	// start a thread that listens for RPCs from worker.go
	c.server()
	log.Println("listening started...")
	// starts a thread that abandons timeout tasks
	go c.loopRemoveTimeoutMapTasks()

	// all are unissued map tasks
	// send to channel after everything else initializes
	log.Printf("file count %d\n", len(files))
	for i := 0; i < len(files); i++ {
		log.Printf("sending %vth file map task to channel\n", i)
		c.unIssuedMapTasks <- i
	}

	return &c
}
