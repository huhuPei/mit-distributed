package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

import "math/rand"
import "sync"
import "time"

type ModeType int

const (
  Map ModeType = iota
  Reduce
  Sleep
)

type StateType int

const (
  Idel StateType = iota
  InProgress
  Completed	
)

type FileInfo struct {
  Name string
  From string
}

type TaskInfo struct {
  Id	    int
  NReduce	int
  Files	[]FileInfo
}

type TaskStatus struct {
  state	 StateType
  workerId string
}

type WorkTask struct {
  status TaskStatus
  info   TaskInfo
}

type Coordinator struct {
  // Your definitions here.
  mu        sync.Mutex
  tpanel    map[ModeType][]WorkTask
  nReduce   int
  nMap      int
  mapRFiles map[int][]FileInfo
}

func randomMillisecond(a int, b int) int {	
  return a + rand.Intn(b-a+1)
}

func (c *Coordinator) PickIdelTask(mode ModeType, workerId string) *TaskInfo {
  wtasks := c.tpanel[mode]
  for i := range wtasks {
    task := &wtasks[i]
    if task.status.state == Idel {
      task.status.state = InProgress
      task.status.workerId = workerId
      go c.waitTaskTimeout(task, mode)
      return &task.info
    }
  }
  return nil
}

func (c *Coordinator) AllMapTasksDone() bool {
  done := false
  c.mu.Lock()
  done = c.nMap == 0
  c.mu.Unlock()
  return done
}

func (c *Coordinator) waitTaskTimeout(wtask *WorkTask, mode ModeType) {
  for tc := 0; tc < 10; tc++ {
    c.mu.Lock()
    if (wtask.status.state == Completed) { tc = 10 }
    c.mu.Unlock()
    time.Sleep(1 * time.Second)
  }
  // worker fault
  // all map tasks of worker need to be re-executed,
  // or only in-process reduce task of worker need to be re-executed				
  c.mu.Lock()
  if wtask.status.state == InProgress {
    faultWorker := wtask.status.workerId
    log.Printf("[Worker] %s Occur fault.", faultWorker)
    if mode == Map {
      for i := range c.tpanel[Map] {
        if c.tpanel[Map][i].status.workerId == faultWorker { 
          log.Printf("[MAP %d] Task need re-exec", i)
          if c.tpanel[Map][i].status.state == Completed { c.nMap++ }
          c.tpanel[Map][i].status.state = Idel
          c.tpanel[Map][i].status.workerId = ""
        }
      }
    } else if mode == Reduce {
      log.Printf("[REDUCE %d] Task need re-exec",  wtask.info.Id)
      wtask.status.state = Idel
      wtask.status.workerId = ""
    }
  }
  c.mu.Unlock()
} 

func (c *Coordinator) PickMapTask(workerId string) (*TaskInfo, int) {
  c.mu.Lock()
  task := c.PickIdelTask(Map, workerId)
  c.mu.Unlock()
  timeout := randomMillisecond(500, 1000) 
  return task, timeout
}

func (c *Coordinator) PickReduceTask(workerId string) (*TaskInfo, int) {
  c.mu.Lock()
  taskInfo := c.PickIdelTask(Reduce, workerId)
  c.mu.Unlock()
  timeout := randomMillisecond(500, 1000)
  if taskInfo != nil && len(taskInfo.Files) == 0 {
    rnum := taskInfo.Id
    for _, rfiles := range c.mapRFiles {
      taskInfo.Files = append(taskInfo.Files, rfiles[rnum]) 
    }
   }  
  return taskInfo, timeout
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
  if !c.AllMapTasksDone() {
    task, nextTimeout := c.PickMapTask(args.WorkerId)
    if task != nil {
      *reply = AssignTaskReply{ Mode: Map, Task: task }
    } else {
      *reply = AssignTaskReply{ Mode: Sleep, Timeout: nextTimeout }
    }
  } else {
    task, nextTimeout := c.PickReduceTask(args.WorkerId)
    if task != nil {
      *reply = AssignTaskReply{ Mode: Reduce, Task: task }
    } else {
      *reply = AssignTaskReply{ Mode: Sleep, Timeout: nextTimeout }
    }
  }
  return nil
}

func (c *Coordinator) CommitTask(args *CommitTaskArgs, reply *CommitTaskReply) error {
  c.mu.Lock()
  task := &c.tpanel[args.Mode][args.TaskId]
  if task.status.state == Completed {
    //do nothing, ignore
  } else if task.status.workerId != args.WorkerId {
    //do nothing, ignore  
  } else {
    task.status.state = Completed
    if args.Mode == Map { 
      c.mapRFiles[args.TaskId] = args.Files
      c.nMap--
      log.Printf("[MAP %d] Task complete. From wokrer:%s", task.info.Id, task.status.workerId)
      if (c.nMap == 0) { log.Println("Coord] All map tasks done!") }
    } else if args.Mode == Reduce { 
      c.nReduce--
      log.Printf("[REDUCE %d] Task complete. From worker:%s", task.info.Id, task.status.workerId)
      if (c.nReduce == 0) { log.Println("[Coord] All reduce tasks done!") }
    }
  }
  c.mu.Unlock()
  return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
  reply.Y = args.X + 1
  return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
  ret := false

  // Your code here.
  c.mu.Lock()
  if (c.nMap + c.nReduce) == 0 { ret = true }
  c.mu.Unlock()
  return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
  c := Coordinator{}

  // Your code here.
  rand.Seed(time.Now().UnixNano())
  
  c.tpanel = make(map[ModeType][]WorkTask, 2)
  c.tpanel[Map] = make([]WorkTask, len(files))
  c.tpanel[Reduce] = make([]WorkTask, nReduce)
  
  for i, filename := range files { 
    c.tpanel[Map][i].status.state = Idel
    c.tpanel[Map][i].info = TaskInfo{ Id: i, NReduce: nReduce }
    c.tpanel[Map][i].info.Files = []FileInfo{ {filename, "mr-gfs"} }
  }
  for i := 0; i < nReduce; i++ {
    c.tpanel[Reduce][i].status.state = Idel
    c.tpanel[Reduce][i].info = TaskInfo{ Id: i }
  }
  c.nMap = len(files)
  c.nReduce = nReduce
  c.mapRFiles = make(map[int][]FileInfo, c.nMap)
  c.server()
  return &c	
}
