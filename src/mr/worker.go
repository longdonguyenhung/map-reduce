package mr

import (
	"fmt"
	"io/ioutil"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type Node struct {
	MapFunction         func(string, string) []KeyValue
	ReduceFunction      func(string, []string) string
	MapResultStorage    MapResultStorage
	WorkerTaskProcedure WorkerTaskProcedure
	Converter           Converter
	NodeIdentity        string
	ResourceProvider    ResourceProvider
	TaskRecordKeeper    TaskRecordKeeper
}

type ResourceProvider interface {
	GetData(location Location) string
}

type LocalResourceProvider struct {
}

func (n *Node) Destroy() {
	//WorkerTasks := n.TaskRecordKeeper.GetAllTask()
	//for _, workerTask := range WorkerTasks {
	//	workerTask.GetTaskProcedure().Destroy()
	//}
	os.Exit(1)
}

func (l LocalResourceProvider) GetData(location Location) string {
	file, err := os.Open(location.File)
	if err != nil {
		log.Fatalf("cannot open %v", location.File)
		return ""
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", location.File)
		return ""
	}
	file.Close()
	return string(content)
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type WorkerEngine struct {
	TaskPool        TaskPool
	TaskProvider    TaskProvider
	SchedulerEngine SchedulerEngine
}

type CoordinatorCommunicator interface {
	SignalDone(task WorkerTask) bool
}

type TaskProvider interface {
	GetTask() WorkerTask
}

type TaskPool interface {
	Receive(Task WorkerTask)
	CanReceiveTask() bool
	Eliminate(Task WorkerTask)
}

type SchedulerEngine interface {
	RunSchedule(scheduleTask ScheduleTask, time int)
}

type ScheduleTask interface {
	Run()
}

type SchedulerJob struct {
	Node             Node
	TaskProvider     TaskProvider
	TaskPool         TaskPool
	mu               sync.Mutex
	TaskRecordKeeper TaskRecordKeeper
}

func (schedulerJob *SchedulerJob) Run() {
	schedulerJob.mu.Lock()
	defer schedulerJob.mu.Unlock()
	if schedulerJob.TaskPool.CanReceiveTask() {
		//log.Println("task pool can receive task", schedulerJob.TaskPool)
		task := schedulerJob.TaskProvider.GetTask()
		if task == nil {
			//log.Println("no task receive destroy node")
			schedulerJob.Node.Destroy()
		}

		if provided, keepTask := schedulerJob.TaskRecordKeeper.HasTaskProvided(task); provided {
			//log.Println("task duplicated", task.GetTaskName())
			if keepTask.GetStatus() == 3 {
				keepTask.GetTaskProcedure().Recovery()
			}
			return
		}
		if task == nil {
			return
		}
		//log.Println("task pool receive new task", task.GetTaskName())
		schedulerJob.TaskPool.Receive(task)
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	simpleTaskPool := SimpleTaskPool{
		ConfiguredConcurrentTaskRun: 3,
		WorkerTasks:                 make([]WorkerTask, 0),
		WorkerTaskProcedure:         WorkerTaskProcedure{},
	}
	workerTaskProcedure := WorkerTaskProcedure{}
	simpleConverter := SimpleConverter{}
	node := Node{
		TaskRecordKeeper: &simpleTaskPool,
		MapFunction:      mapf,
		ReduceFunction:   reducef,
		MapResultStorage: &GeneralMapResultStorage{
			Converter: simpleConverter,
		},
		WorkerTaskProcedure: workerTaskProcedure,
		Converter:           simpleConverter,
		NodeIdentity:        strconv.Itoa(rand.IntN(10000)),
		ResourceProvider:    &LocalResourceProvider{},
	}
	coordinatorClient := CoordinatorClient{
		TaskFactories: []TaskWorkerFactory{&MapWorkerTaskFactory{node: node}, &ReduceWorkerTaskFactory{node: node}},
		Node:          node,
	}
	WorkerEngine := WorkerEngine{
		TaskPool: &SimpleTaskPool{
			ConfiguredConcurrentTaskRun: 3,
			WorkerTasks:                 nil,
			WorkerTaskProcedure:         workerTaskProcedure,
		},
		TaskProvider:    &coordinatorClient,
		SchedulerEngine: &SimpleSchedulerEngine{},
	}

	SchedulerJob := SchedulerJob{
		mu:               sync.Mutex{},
		TaskProvider:     &coordinatorClient,
		TaskPool:         &simpleTaskPool,
		TaskRecordKeeper: &simpleTaskPool,
		Node:             node}
	WorkerEngine.SchedulerEngine.RunSchedule(&SchedulerJob, 10)
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
