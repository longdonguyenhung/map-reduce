package mr

import (
	"strconv"
)

type CoordinatorClient struct {
	TaskFactories []TaskWorkerFactory
	Node          Node
}

func (c *CoordinatorClient) SignalDone(task WorkerTask) bool {
	//log.Println("signal done for task", task.GetTaskName())
	taskReply := TaskReplyDone{}
	return call("Coordinator.OnTaskDone", &TaskRequestDone{
		ResourceType: task.GetTaskType(),
		ResourceId:   task.GetResource().GetUniqueId(),
		Result:       task.GetTaskResult(),
	}, &taskReply)
}

type TaskWorkerFactory interface {
	Transform(task WorkerTask) Resource
}

type MapWorkerTaskFactory struct {
	node Node
}

func (resultFactory *MapWorkerTaskFactory) Transform(task WorkerTask) Resource {
	mapTask := task.(*MapWorkerTask)
	return &Chunk{
		file:    "",
		status:  0,
		results: mapTask.ResultDataStorages,
		node:    Node{},
	}
}

type ReduceWorkerTaskFactory struct {
	node Node
}

func (r *ReduceWorkerTaskFactory) Transform(task WorkerTask) Resource {
	reduceTask := task.(*ReduceWorkerTask)
	return &ReduceResource{
		storageLocations: nil,
		reduceNumberId:   reduceTask.ReducerTaskNum,
		status:           0,
		result:           reduceTask.Result,
	}
}

func (c *CoordinatorClient) GetTask() WorkerTask {
	taskReply := TaskReply{}
	if (!call("Coordinator.ReceiveRequest", &TaskRequest{NodeId: c.Node.NodeIdentity}, &taskReply)) {
		//log.Println("[CoordinatorClient] receive request and reply", taskReply)
		return nil
	}
	//log.Println("Reply", taskReply)
	if taskReply.TaskType == 0 {
		return &MapWorkerTask{
			Status: 0,
			TaskContext: TaskContext{
				CoordinatorCommunicator: c,
				Node:                    c.Node,
			},
			MapFunction:         c.Node.MapFunction,
			ReducerNumber:       taskReply.ReduceNumber,
			ResultStorage:       c.Node.MapResultStorage,
			FileName:            taskReply.Files[0].File,
			Content:             "",
			WorkerTaskProcedure: c.Node.WorkerTaskProcedure,
			TaskName:            strconv.Itoa(taskReply.TaskId),
			ResultDataStorages:  nil,
		}
	}

	if taskReply.TaskType == 1 {
		return &ReduceWorkerTask{
			TaskContext: TaskContext{
				CoordinatorCommunicator: c,
				Node:                    c.Node,
			},
			ReduceInputData:     taskReply.Files,
			KVA:                 nil,
			Status:              0,
			ReduceFunction:      c.Node.ReduceFunction,
			ReducerTaskNum:      taskReply.TaskId,
			WorkerTaskProcedure: WorkerTaskProcedure{},
			TaskName:            strconv.Itoa(taskReply.TaskId),
			Converter:           c.Node.Converter,
			Result:              "",
		}
	}

	return nil
}
