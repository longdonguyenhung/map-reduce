package mr

import (
	"log"
	"os"
	"strconv"
	"sync"
)

// 0 is where resource data is store, 1 is where resource result processed is store
type Resource interface {
	UpdateStatus(status int)
	GetDataLocation() []Location
	MarkDirty()
	GetStatus() int
	GetUniqueId() int
}

type Task interface {
	GetAuthor() Node
}

type Location struct {
	File string
}

type Chunk struct {
	file     string
	status   int
	results  []Result
	node     Node
	id       int
	priority int
	mu       sync.Mutex
}

func (c *Chunk) GetAuthor() Node {
	return c.node
}

type Result struct {
	ReduceNumberId int
	File           string
}

func (c *Chunk) GetAddress() string {
	return c.file
}

func (c *Chunk) UpdateStatus(status int) {
	//c.mu.Lock()
	//defer c.mu.Unlock()
	c.status = status
}

func (c *Chunk) GetStatus() int {
	//c.mu.Lock()
	//defer c.mu.Unlock()
	return c.status
}

func (c *Chunk) GetDataLocation() []Location {
	var locations []Location
	//log.Println(c.file)
	locations = append(locations, Location{File: c.file})
	return locations
}

func (c *Chunk) MarkDirty() {
	c.status = 0
}

type ReduceResource struct {
	mu               sync.Mutex
	storageLocations []Location
	reduceNumberId   int
	status           int
	result           string
	Node             Node
	priority         int
}

func (r *ReduceResource) UpdateStatus(status int) {
	//r.mu.Lock()
	//defer r.mu.Unlock()
	r.status = status
}

func (r ReduceResource) GetDataLocation() []Location {
	return r.storageLocations
}

func (r ReduceResource) MarkDirty() {
	r.status = 3
}

func (r ReduceResource) GetStatus() int {
	//r.mu.Lock()
	//defer r.mu.Unlock()
	return r.status
}

func (resource *ReduceResource) changeTempFileName(r Resource) {
	reduce := r.(*ReduceResource)
	os.Rename(reduce.result, "mr-out-"+strconv.Itoa(reduce.reduceNumberId)+".txt")
	log.Println("output file ", "mr-out-"+strconv.Itoa(reduce.reduceNumberId)+".txt")
}

func (c *Chunk) GetUniqueId() int {
	return c.id
}

func (r ReduceResource) GetUniqueId() int {
	return r.reduceNumberId
}

func (r ReduceResource) GetAuthor() Node {
	return r.Node
}

func (c *Chunk) GetPriority() int {
	return c.priority
}

func (c *Chunk) DecreasePriority() {
	c.priority--
}

func (r ReduceResource) GetPriority() int {
	return r.priority
}

func (r ReduceResource) DecreasePriority() {
	r.priority--
}
