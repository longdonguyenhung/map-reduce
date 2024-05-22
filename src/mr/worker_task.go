package mr

import (
	"fmt"
	"os"
	"sort"
	"strconv"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type TaskContext struct {
	CoordinatorCommunicator CoordinatorCommunicator
	Node                    Node
}

type MapWorkerTask struct {
	Status              int
	TaskContext         TaskContext
	MapFunction         func(string, string) []KeyValue
	ReducerNumber       int
	ResultStorage       MapResultStorage
	FileName            string
	Content             string
	WorkerTaskProcedure WorkerTaskProcedure
	TaskName            string
	ResultDataStorages  []Result //file where it store the data
}

func (m *MapWorkerTask) GetTaskResult() []Result {
	return m.ResultDataStorages
}

func (m *MapWorkerTask) GetTaskName() string {
	return m.TaskName
}

func (m *MapWorkerTask) GetStatus() int {
	return m.Status
}

func (m *MapWorkerTask) Recovery() {
	m.WorkerTaskProcedure.Recovery(m)
}

func (m *MapWorkerTask) Destroy() bool {
	for _, result := range m.ResultDataStorages {
		os.Remove(result.File)
	}
	return true
}

type MapResultStorage interface {
	Store([][]KeyValue) []Result
}

type WorkerTaskProcedure struct {
}

type WorkerTask interface {
	Retryable() bool
	GetStatus() int
	GetTaskName() string
	GetTaskType() int
	GetTaskResult() []Result
	GetResource() Resource
	GetTaskProcedure() TaskProcedure
}

type TaskProcedure interface {
	MarkInProgress()
	ClaimResource() bool
	Suspend()
	Progress()
	OnDone()
	Recovery()
	Destroy() bool
}

func (workerTask *WorkerTaskProcedure) Do(m WorkerTask) {
	taskProcedure := m.GetTaskProcedure()
	taskProcedure.MarkInProgress()
	if taskProcedure.ClaimResource() {
		//suspend this task and will recover this task when have chance
		taskProcedure.Suspend()
	}
	taskProcedure.Progress()
	taskProcedure.OnDone()
}

func (workerTask *WorkerTaskProcedure) Recovery(m WorkerTask) {
	if m.Retryable() {
		workerTask.Do(m)
	}
}

func (m *MapWorkerTask) ClaimResource() bool {
	data := m.TaskContext.Node.ResourceProvider.GetData(Location{
		File: m.FileName,
	})
	if data == "" {
		return false
	}
	m.Content = data
	return true
}

func (m *MapWorkerTask) Suspend() {
	m.Status = 3
}

func (m *MapWorkerTask) Progress() {
	keyValues := m.MapFunction(m.FileName, m.Content)
	resultData := make([][]KeyValue, m.ReducerNumber)
	for _, keyValue := range keyValues {
		reducerNumber := ihash(keyValue.Key) % m.ReducerNumber
		resultData[reducerNumber] = append(resultData[reducerNumber], keyValue)
	}
	m.ResultDataStorages = m.ResultStorage.Store(resultData)
}

func (m *MapWorkerTask) OnDone() {
	m.Status = 2
	m.TaskContext.CoordinatorCommunicator.SignalDone(m)
}

func (m *MapWorkerTask) Retryable() bool {
	return false
}

// 0 is not start, 1 is in progress, 2 is done and 3 is suspend
func (m *MapWorkerTask) MarkInProgress() {
	m.Status = 1
}

type ReduceWorkerTask struct {
	TaskContext         TaskContext
	ReduceInputData     []Location
	KVA                 []KeyValue
	Status              int
	ReduceFunction      func(string, []string) string
	ReducerTaskNum      int
	WorkerTaskProcedure WorkerTaskProcedure
	TaskName            string
	Converter           Converter
	Result              string
}

func (m *ReduceWorkerTask) GetTaskResult() []Result {
	var results []Result
	results = append(results, Result{
		ReduceNumberId: m.ReducerTaskNum,
		File:           m.Result,
	})
	return results
}

func (m *ReduceWorkerTask) GetTaskType() int {
	return 1
}

func (m *ReduceWorkerTask) Destroy() bool {
	//gently destroy it, for further development it should communicate to the coordinator
	//to inform that the job is canceled
	//however in this job the destroy is not likely to use in interrupted manner
	os.Remove(m.Result)
	return true
}

func (m *ReduceWorkerTask) GetTaskName() string {
	return m.TaskName
}

func (m *ReduceWorkerTask) GetStatus() int {
	return m.Status
}

func (m *ReduceWorkerTask) MarkInProgress() {
	m.Status = 1
}

func (m *ReduceWorkerTask) Suspend() {
	m.Status = 3
}

func (m *ReduceWorkerTask) Progress() {
	sort.Sort(ByKey(m.KVA))

	oname := "mr-temp-" + strconv.Itoa(m.ReducerTaskNum)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(m.KVA) {
		j := i + 1
		for j < len(m.KVA) && m.KVA[j].Key == m.KVA[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, m.KVA[k].Value)
		}
		output := m.ReduceFunction(m.KVA[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", m.KVA[i].Key, output)
		i = j
	}
	m.Result = oname

	ofile.Close()
}

func (m *ReduceWorkerTask) Retryable() bool {
	return true
}

func (m *ReduceWorkerTask) Recovery() {
	m.WorkerTaskProcedure.Recovery(m)
}

type ReduceInputData struct {
	FileName string
	Node     Node
}

func (m *ReduceWorkerTask) ClaimResource() bool {
	for _, chunk := range m.ReduceInputData {
		data := m.TaskContext.Node.ResourceProvider.GetData(chunk)
		if data == "" {
			return false
		}
		kva := m.Converter.Deserialize(data)
		m.KVA = append(m.KVA, kva...)
	}
	return true
}

func (m *ReduceWorkerTask) OnDone() {
	if !m.TaskContext.CoordinatorCommunicator.SignalDone(m) {
		//if fail to signal done, store it the result in somewhere that we later can retrieve
		//however it is unlikely in host that the communication be failed
	}
	m.Status = 2
}

func (m *MapWorkerTask) GetTaskType() int {
	return 0
}

func (m *MapWorkerTask) GetResource() Resource {
	if id, err := strconv.Atoi(m.TaskName); err != nil {
		return nil
	} else {
		return &Chunk{
			file:    m.FileName,
			results: m.ResultDataStorages,
			id:      id,
		}
	}
}

func (m *MapWorkerTask) GetTaskProcedure() TaskProcedure {
	return m
}

func (m *ReduceWorkerTask) GetResource() Resource {
	return &ReduceResource{
		reduceNumberId: m.ReducerTaskNum,
		result:         m.Result,
	}
}

func (m *ReduceWorkerTask) GetTaskProcedure() TaskProcedure {
	return m
}
