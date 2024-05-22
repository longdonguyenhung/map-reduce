package mr

import (
	"container/heap"
	"errors"
	"log"
	"math/rand/v2"
	"sync"
)

import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	ResourceManager ResourceManager
	ReduceNumber    int
	mu              sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

type TaskRequest struct {
	NodeId string
}

type TaskReply struct {
	TaskType     int
	Files        []Location
	ReduceNumber int
	TaskId       int
}

type SendResource struct {
	taskType  int
	inputFile []string
}

// 0 equal new, 1 equal in-progress and 2 equal done
func (c *Coordinator) ReceiveRequest(request *TaskRequest, reply *TaskReply) error {
	resource := c.ResourceManager.GetNext()
	if resource == nil {
		return errors.New("resource not found")
	}
	//log.Println("receive request", resource.GetUniqueId(), resource.GetStatus())
	if _, ok := resource.(*Chunk); ok {
		reply.TaskType = 0
	} else {
		reply.TaskType = 1
	}

	reply.TaskId = resource.GetUniqueId()
	reply.Files = resource.GetDataLocation()
	reply.ReduceNumber = c.ReduceNumber
	return nil
}

type TaskRequestDone struct {
	ResourceType int
	ResourceId   int
	Result       []Result
}

type TaskReplyDone struct {
	Receive bool
}

func (c *Coordinator) OnTaskDone(args *TaskRequestDone, reply *TaskReplyDone) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var resource Resource
	if args.ResourceType == 0 {
		resource = &Chunk{
			results:  args.Result,
			id:       args.ResourceId,
			priority: 0,
		}
	} else {
		resource = &ReduceResource{
			reduceNumberId: args.ResourceId,
			result:         args.Result[0].File,
		}
	}
	c.ResourceManager.MarkDone(resource)
	return nil
}

type NodeUnreachable struct {
	Author Node
	Node   Node
}

type NodeUnReachReply struct {
	Task Task
}

func (c *Coordinator) NodeUnReach(args *NodeUnreachable, reply *NodeUnReachReply) error {
	resources := c.ResourceManager.GetResourceFromNode(args.Node)
	for _, resource := range resources {
		resource.MarkDirty()
	}
	return nil
}

type UpdateTaskLocation struct {
	Resource Resource
}

type UpdateTaskReply struct {
	Locations []Location
}

func (c *Coordinator) UpdateTaskLocation(args *UpdateTaskLocation, reply *UpdateTaskReply) error {
	resource := c.ResourceManager.GetResource(args.Resource)
	reply.Locations = (*resource).GetDataLocation()
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
	return c.ResourceManager.IsAllResourceDone()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	ResourceHeap := ResourceHeap{}
	var Chunks []*Chunk
	for _, file := range files {
		chunk := Chunk{
			file:     file,
			status:   0,
			id:       rand.IntN(1000) + nReduce,
			priority: 10000,
			mu:       sync.Mutex{},
		}
		Chunks = append(Chunks, &chunk)
		ResourceHeap.Push(&chunk)
	}

	var ReduceResources []*ReduceResource
	for i := 0; i < nReduce; i++ {
		reduce := ReduceResource{
			reduceNumberId: i,
			status:         0,
			priority:       0,
			mu:             sync.Mutex{},
		}
		ReduceResources = append(ReduceResources, &reduce)
		ResourceHeap.Push(&reduce)
	}

	heap.Init(&ResourceHeap)

	c := Coordinator{
		mu:           sync.Mutex{},
		ReduceNumber: nReduce,
		ResourceManager: &PriorityResourceManager{
			Chunks:          Chunks,
			ReduceResources: ReduceResources,
			ResourceHeap:    ResourceHeap,
		},
	}
	c.server()
	return &c
}
