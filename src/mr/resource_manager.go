package mr

import (
	"container/heap"
	"log"
	"sync"
)

type ResourceManager interface {
	GetNext() Resource
	IsAllResourceDone() bool
	MarkDone(Resource) Resource
	GetResource(Resource Resource) *Resource
	GetResourceFromNode(Node Node) []Resource
}

type PriorityResourceManager struct {
	Chunks          []*Chunk
	ReduceResources []*ReduceResource
	ResourceHeap    ResourceHeap
	mutex           sync.Mutex
}

type PriorityResource interface {
	Resource
	GetPriority() int
	DecreasePriority()
}

// An IntHeap is a min-heap of ints.
type ResourceHeap []interface{}

func (h ResourceHeap) Len() int { return len(h) }
func (h ResourceHeap) Less(i, j int) bool {
	return h[i].(PriorityResource).GetPriority() > h[j].(PriorityResource).GetPriority()
}
func (h ResourceHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *ResourceHeap) Push(x interface{}) {
	*h = append(*h, x.(PriorityResource))
}

func (h *ResourceHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (resourceManager *PriorityResourceManager) GetNext() Resource {
	resourceManager.mutex.Lock()
	defer resourceManager.mutex.Unlock()
	for {
		if resourceManager.ResourceHeap.Len() == 0 {
			return nil
		}
		resource := heap.Pop(&resourceManager.ResourceHeap).(PriorityResource)
		if resource.GetStatus() == 2 {
			//log.Println("resource has finish", resource)
			continue
		}

		resource.DecreasePriority()
		heap.Push(&resourceManager.ResourceHeap, resource)

		if reduce, ok := resource.(*ReduceResource); ok {
			var data []Location
			for _, chunk := range resourceManager.Chunks {
				for _, result := range chunk.results {
					if result.ReduceNumberId == reduce.reduceNumberId {
						data = append(data, Location{File: result.File})
					}
				}
			}
			reduce.storageLocations = data
			log.Println("number of data", len(reduce.storageLocations))
			log.Println("data", reduce.storageLocations)
			return reduce
		}
		return resource
	}
}

func (resourceManager *PriorityResourceManager) IsAllResourceDone() bool {
	for _, reduceResource := range resourceManager.ReduceResources {
		if reduceResource.GetStatus() == 2 {
			return true
		}
	}
	return false
}

func (resourceManager *PriorityResourceManager) MarkDone(r Resource) Resource {
	//resourceManager.mutex.Lock()
	//defer resourceManager.mutex.Unlock()
	log.Println("number of remaining task: ", len(resourceManager.ResourceHeap))
	for _, reduceResource := range resourceManager.ReduceResources {
		if reduceResource.GetUniqueId() == r.GetUniqueId() {
			if reduceResource.GetStatus() == 2 {
				return reduceResource
			}
			log.Println("reduceResource", reduceResource.GetStatus())
			reduceResource.UpdateStatus(2)
			reduceResource.changeTempFileName(r)
			return reduceResource
		}
	}

	for _, chunk := range resourceManager.Chunks {
		if chunk.GetUniqueId() == r.GetUniqueId() {
			if chunk.GetStatus() == 2 {
				return chunk
			}
			chunk.UpdateStatus(2)
			chunk.results = r.(*Chunk).results
			return chunk
		}
	}

	return nil
}

func (resourceManager *PriorityResourceManager) GetAllResources() []Resource {
	var resources []Resource
	for _, chunk := range resourceManager.Chunks {
		resources = append(resources, chunk)
	}

	for _, reduceResource := range resourceManager.ReduceResources {
		resources = append(resources, reduceResource)
	}
	return resources
}

func (resourceManager *PriorityResourceManager) find(r Resource) *Resource {
	for _, resource := range resourceManager.GetAllResources() {
		if resource.GetUniqueId() == r.GetUniqueId() {
			return &resource
		}
	}
	return nil
}

func (resourceManager *PriorityResourceManager) GetResource(Resource Resource) *Resource {
	return resourceManager.find(Resource)
}

func (resourceManager *PriorityResourceManager) GetResourceFromNode(Node Node) []Resource {
	var result []Resource
	for _, task := range resourceManager.Chunks {
		if task.GetAuthor().NodeIdentity == Node.NodeIdentity {
			result = append(result, task)
		}
	}

	for _, task := range resourceManager.ReduceResources {
		if task.GetAuthor().NodeIdentity == Node.NodeIdentity {
			result = append(result, task)
		}
	}
	return result
}
