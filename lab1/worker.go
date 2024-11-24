package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

import "time"
import "io/ioutil"
import "os"
import "strings"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
  Key   string
  Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
  h := fnv.New32a()
  h.Write([]byte(key))
  return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func getWorkerID() string {
  tmpfile, err := ioutil.TempFile("", "mr-worker-")
  if err != nil {
    return ""
  }
  lvs := strings.Split(tmpfile.Name(), "/")
    return lvs[len(lvs)-1]
}

func procMapTask(task *TaskInfo, mapf func(string, string) []KeyValue) []string {
  filename := task.Files[0].Name
  log.Printf("[MAP %d] Map filename: %s", task.Id, filename)
  file, err := os.Open(filename)
  defer file.Close()
  if err != nil {
    log.Fatalf("[MAP %d] Error cannot open %s", task.Id, filename)
  }
  contents, err := ioutil.ReadAll(file)
  if err != nil {
    log.Fatalf("[MAP %d] Error cannot read %v", task.Id, filename)
  }
  intermediate := mapf(filename, string(contents))

  tmpfiles := make([]*os.File, task.NReduce)
  for i := 0; i < len(intermediate); i++ {
    r := ihash(intermediate[i].Key) % task.NReduce
    if tmpfiles[r] == nil {
      prefix := fmt.Sprintf("mr-%d-%d-", task.Id, r)
      tmpfile, err := ioutil.TempFile("", prefix)
      defer tmpfile.Close()
      if err != nil {
        log.Printf("[MAP %d] Error tmp file:R%d create failed.", task.Id, r)
      }
      tmpfiles[r] = tmpfile
    }
    fmt.Fprintf(tmpfiles[r], "%v %v\n", intermediate[i].Key, intermediate[i].Value)
  }
  filenames := make([]string, task.NReduce)
  for r := 0; r < task.NReduce; r++ {
    if tmpfiles[r] != nil {
      rfilename := fmt.Sprintf("mr-%d-%d", task.Id, r)
      filenames[r] = rfilename
      err := os.Rename(tmpfiles[r].Name(), rfilename)  
      if err != nil {  
        log.Printf("[MAP %d] Error rename tmp file:R%d failed.", task.Id, r)  
      }
    }
  }
  return filenames
}

func parseKeyValues(contents string) (kvs []KeyValue) {
  lines := strings.Split(contents, "\n")
  for _, line := range lines {
    if line != "" {
      pair := strings.Split(line, " ")
      kvs = append(kvs, KeyValue{ Key: pair[0], Value: pair[1] }) 
    }
  }
  return kvs
}

func loadReduceKeyValues(taskId int, filenames []string) (kvs []KeyValue) {
  for _, filename := range filenames {
    log.Printf("[REDUCE %d] Intermediate filename: %s", taskId, filename)
    contents, err := ioutil.ReadFile(filename)
    if err != nil {
      log.Fatalf("[REDUCE %d] Error reading %s: %v",
       taskId, filename, err)
    }
    newKVs := parseKeyValues(string(contents))
    if len(newKVs) == 0 {
      log.Printf("[REDUCE %d] Warning: file %s was empty!?", taskId, filename)
    }
    kvs = append(kvs, newKVs...)
  }
  return kvs
}

func procReduceTask(task *TaskInfo, reducef func(string, []string) string) string {
  filenames := []string{}
  for _, file := range task.Files {
    if file.Name != "" {
      filenames = append(filenames, file.Name)
    }
  }
  intermediate := loadReduceKeyValues(task.Id, filenames)
  // sort kvs
  sort.Sort(ByKey(intermediate))
  oname := fmt.Sprintf("mr-out-%d", task.Id) 
  tmpfile, err := ioutil.TempFile("", oname + "-")
  defer tmpfile.Close()
  i := 0
  for i < len(intermediate) {
    j := i + 1
    for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
      j++
    }
    values := []string{}
    for k := i; k < j; k++ {
      values = append(values, intermediate[k].Value)
    }
    output := reducef(intermediate[i].Key, values)

    // this is the correct format for each line of Reduce output.
    fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)
    i = j
  }
  err = os.Rename(tmpfile.Name(), oname)
  if err != nil {  
    log.Printf("[REDUCE %d] rename tmp file to %s failed.", task.Id, oname)  
  }
  return oname
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
  reducef func(string, []string) string) {

  // Your worker implementation here.
  wid := getWorkerID()
  if wid == "" { 
    log.Fatalf("[Worker] Error get wid failed, worker run failed")
  }
  log.Printf("[Worker] %s Start up...", wid)
  finished := false
  time.Sleep(2 * time.Second)
  for !finished {
    args := AssignTaskArgs{ WorkerId: wid }
    reply := AssignTaskReply{}
    if ok := call("Coordinator.AssignTask", &args, &reply); ok {
      if reply.Mode == Sleep {
        // log.Printf("[SLEEP] Wait available task.")
        time.Sleep(time.Duration(reply.Timeout) * time.Millisecond)
      } else if reply.Mode == Map {
        rfnames := procMapTask(reply.Task, mapf)
        files := make([]FileInfo, reply.Task.NReduce)
        for r, fname := range rfnames {
          files[r] = FileInfo{ Name: fname, From: wid}
        }
        cargs := CommitTaskArgs{ Mode: reply.Mode, TaskId:reply.Task.Id, WorkerId: wid, Files: files}
        creply := CommitTaskReply{}
        ok := call("Coordinator.CommitTask", &cargs, &creply)
        if !ok { finished = true }
      } else if reply.Mode == Reduce {
        procReduceTask(reply.Task, reducef)
        cargs := CommitTaskArgs{ Mode: reply.Mode, TaskId:reply.Task.Id, WorkerId: wid}
        creply := CommitTaskReply{}
        ok := call("Coordinator.CommitTask", &cargs, &creply)
        if !ok { finished = true }
      }
    } else {
      finished = true
    }
  }
  log.Printf("[Worker] %s Exit...\n", wid)
  // uncomment to send the Example RPC to the coordinator.
  //CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

  // declare an argument structure.
  args := ExampleArgs{}

  // fill in the argument(s).
  args.X = 99

  // declare a reply structure.
  reply := ExampleReply{}

  // send the RPC request, wait for the reply.
  // the "Coordinator.Example" tells the
  // receiving server that we'd like to call
  // the Example() method of struct Coordinator.
  ok := call("Coordinator.Example", &args, &reply)
  if ok {
    // reply.Y should be 100.
    fmt.Printf("reply.Y %v\n", reply.Y)
  } else {
    fmt.Printf("call failed!\n")
  }
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
  // c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
  sockname := coordinatorSock()
  c, err := rpc.DialHTTP("unix", sockname)
  if err != nil {
    log.Fatal("dialing:", err)
  }
  defer c.Close()

  err = c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}
