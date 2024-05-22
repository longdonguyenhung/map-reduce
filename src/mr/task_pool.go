package mr

// this task pool is lack control to the task that
type SimpleTaskPool struct {
	ConfiguredConcurrentTaskRun int
	WorkerTasks                 []WorkerTask
	WorkerTaskProcedure         WorkerTaskProcedure
	TaskRecords                 []WorkerTask
}

func (s *SimpleTaskPool) GetAllTask() []WorkerTask {
	return s.WorkerTasks
}

type TaskRecordKeeper interface {
	HasTaskProvided(WorkerTask) (bool, WorkerTask)
	GetAllTask() []WorkerTask
}

func (s *SimpleTaskPool) HasTaskProvided(task WorkerTask) (bool, WorkerTask) {
	for _, workerTask := range s.WorkerTasks {
		if workerTask.GetTaskName() == task.GetTaskName() {
			return true, workerTask
		}
	}
	return false, nil
}

func (s *SimpleTaskPool) Receive(Task WorkerTask) {
	s.WorkerTasks = append(s.WorkerTasks, Task)
	s.TaskRecords = append(s.TaskRecords, Task)
	//trigger task run, however this function should be work in async manner
	s.WorkerTaskProcedure.Do(Task)
}

func (s SimpleTaskPool) CanReceiveTask() bool {
	numberTaskRun := 0
	for _, task := range s.WorkerTasks {
		status := task.GetStatus()
		if status == 1 {
			numberTaskRun += 1
		} else if status == 2 {
			task.GetTaskProcedure().Recovery()
		}
	}
	return numberTaskRun < s.ConfiguredConcurrentTaskRun
}

func (s SimpleTaskPool) Eliminate(Task WorkerTask) {
	for index, task := range s.WorkerTasks {
		if task.GetTaskName() == Task.GetTaskName() {
			Task.GetTaskProcedure().Destroy()
			//wait until task is destroy then eliminate it from task pool
			s.WorkerTasks = append(s.WorkerTasks[:index], s.WorkerTasks[index+1:]...)
		}
	}
}
