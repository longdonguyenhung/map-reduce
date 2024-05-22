package mr

import "time"

type SimpleSchedulerEngine struct {
}

func (schedulerEngine *SimpleSchedulerEngine) RunSchedule(job ScheduleTask, interval int) {
	for {
		time.Sleep(time.Duration(interval) * time.Second)
		go job.Run()
	}
}
