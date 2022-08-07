package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

//任务阶段

type jobStage uint

const (
	mapStage    jobStage = iota //map阶段
	reduceStage                 //reduce阶段
)

type Master struct {
	// Your definitions here.
	lock        *sync.Mutex
	stage       jobStage
	mapTasks    map[string]*taskState
	reduceTasks map[string]*taskState
	finished    bool
	nReduce     int
}

//任务状态信息
type taskState struct {
	name      string
	taskType  TaskType
	startTime time.Time
	finished  bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//分发任务
func (m *Master) DistributeTask(args *DistributeTaskArgs, reply *DistrubuteTaskReply) error {
	reply.NReduce = m.nReduce
	m.lock.Lock()
	defer m.lock.Unlock()
	var tasks map[string]*taskState
	if mapStage == m.stage {
		tasks = m.mapTasks
	} else {
		tasks = m.reduceTasks
	}
	for _, taskState := range tasks {
		//任务没有完成，并且任务开始时间为0或者任务开始超过10s没有完成可以重新分发
		if !taskState.finished && (taskState.startTime.IsZero() || time.Since(taskState.startTime).Seconds() > 10) {
			reply.FileName = taskState.name
			reply.TaskType = taskState.taskType
			reply.NReduce = m.nReduce
			taskState.startTime = time.Now()
			return nil
		}
	}
	reply.TaskType = Unkown
	return nil
}

//完成任务
func (m *Master) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	var taskState *taskState
	if args.TaskType == Map {
		taskState = m.mapTasks[args.FileName]
	} else {
		taskState = m.reduceTasks[args.FileName]
	}
	taskState.finished = true
	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.finished
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	m.lock = new(sync.Mutex)
	m.finished = false
	m.nReduce = nReduce
	m.stage = mapStage
	m.initMapTaskStates(files)
	m.initReduceTaskStates(nReduce)
	//启动端口
	m.server()
	//异步检查任务状态
	go m.checkJobState()
	return &m
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) checkJobState() {
	time.Sleep(time.Second)
	for {
		m.lock.Lock()
		if m.stage == mapStage {
			allMapTaskFinished := true
			for _, taskState := range m.mapTasks {
				if !taskState.finished {
					allMapTaskFinished = false
					break
				}
			}
			if allMapTaskFinished {
				m.stage = reduceStage
			}
		} else {
			allReduceFinished := true
			for _, taskState := range m.reduceTasks {
				if !taskState.finished {
					allReduceFinished = false
					break
				}
			}
			if allReduceFinished {
				m.finished = true
			}
		}
		m.lock.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (m *Master) initMapTaskStates(files []string) {
	m.mapTasks = make(map[string]*taskState)
	for _, file := range files {
		m.mapTasks[file] = &taskState{
			name:     file,
			taskType: Map,
			finished: false,
		}
	}
}

func (m *Master) initReduceTaskStates(nReduce int) {
	m.reduceTasks = make(map[string]*taskState)
	for i := 0; i < nReduce; i++ {
		name := strconv.Itoa(i)
		m.reduceTasks[name] = &taskState{
			name:     name,
			taskType: Reduce,
			finished: false,
		}
	}
}