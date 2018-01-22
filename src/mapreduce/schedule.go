package mapreduce

import (
	"fmt"
	"sync"
)

const (
	taskQueueSize = 10
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var nOther int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		ntasks = nReduce
		nOther = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nOther)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//

	taskCh := make(chan interface{}, taskQueueSize)
	wg := &sync.WaitGroup{}

	// set unfinished task counter
	wg.Add(ntasks)
	// add all tasks to queue
	go addTasks(taskCh, jobName, ntasks, nOther, phase, mapFiles)

	go func() {
		for server := range registerChan {
			// add newly registered server to available queue
			fmt.Println("registered server: " + server)
			// one go routine per server
			// TODO consider better solution
			go handleTask(taskCh, server, wg)
		}
	}()

	// wait for all tasks to finish
	wg.Wait()
	// close channels
	close(taskCh)

	fmt.Printf("Schedule: %v phase done\n", phase)
}

func handleTask(taskCh chan interface{}, server string, wg *sync.WaitGroup) {
	// retrieve task
	for task := range taskCh {
		// send task to remote server
		taskFinished := call(server, "Worker.DoTask", task, nil)
		if taskFinished {
			// reduce unfinished task counter by one
			wg.Done()
		} else {
			// TODO find a better solution or check if this is ok
			go func(task interface{}) {
				taskCh <- task
			}(task)
		}
	}

	debug("no more task on server: %s", server)
}

func addTasks(taskCh chan interface{}, jobName string, ntasks int, nOther int, phase jobPhase, mapFiles []string) {
	tasks := getTasks(jobName, ntasks, nOther, phase, mapFiles)

	for _, task := range tasks {
		task := task
		taskCh <- task
	}
}

func getTasks(jobName string, ntasks int, nOther int, phase jobPhase, mapFiles []string) []interface{} {
	tasks := make([]interface{}, ntasks)
	for i := 0; i < ntasks; i++ {
		mapFile := ""
		if phase == mapPhase {
			mapFile = mapFiles[i]
		}
		tasks[i] = &DoTaskArgs{
			JobName:       jobName,
			File:          mapFile,
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: nOther}
	}
	return tasks
}
