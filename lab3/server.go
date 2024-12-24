package kvraft

import (
  "6.824/labgob"
  "6.824/labrpc"
  "6.824/raft"
  "log"
  "sync"
  "sync/atomic"
  //"bytes"
  "fmt"
  "time"
)

const Debug = false

type logTopic string
const (
  dGet 		 logTopic = "GET"
  dPut		 logTopic = "PUT"
  dApply   logTopic = "APY"
  dError   logTopic = "ERRO"
  dInfo    logTopic = "INFO"
  dPersist logTopic = "PERS"
  dSnap    logTopic = "SNAP"
  dTerm    logTopic = "TERM"
  dTest    logTopic = "TEST"
  dWarn    logTopic = "WARN"
)

func DPrintf(topic logTopic, format string, a ...interface{}) (n int, err error) {
  if Debug {
    prefix := fmt.Sprintf("%v ", string(topic))
    format = prefix + format
    if topic == dError {
      log.Fatalf(format, a...)
    } else {
      log.Printf(format, a...)
    }
  }
  return
}


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Name  string // "Put" / "Append" / "Get"
  Key   string
  Value string
  SequenceNumber int
  ClientId int64
}

//todo::

// 编码函数

// 解码函数

type void struct {}

type KVServer struct {
  mu      sync.Mutex
  me      int
  rf      *raft.Raft
  applyCh chan raft.ApplyMsg
  dead    int32 // set by Kill()

  maxraftstate int // snapshot if log grows this big

  // Your definitions here.
  data				 map[string]string
  maxSeqNumberOfClients map[int64]int
  syncIndexChs		 map[int]chan Op
}

func (kv *KVServer) getIndexCh(index int) chan Op {	
  kv.mu.Lock()
  defer kv.mu.Unlock()
  if _, exist := kv.syncIndexChs[index]; !exist {
    kv.syncIndexChs[index] = make(chan Op, 1)
  }
  return kv.syncIndexChs[index]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
  // Your code here.
   op := Op{Name: "Get", Key: args.Key, SequenceNumber: args.SequenceNumber, 
    ClientId: args.ClientId}
  // wbuf := new(bytes.Buffer)
  // enc := labgob.NewEncoder(wbuf)
  // if enc.Encode(op) != nil {
  // 	DFatalf("Op encode fail, unable to start cmd:%v", op)
  // }
  // cmd := wbuf.Bytes()
  index, term, isLeader := kv.rf.Start(op)
  DPrintf(dGet, "Se%d T%d Start Op{Get %s} Index:%d from SN:%d Cli-%d", 
    kv.me, term, op.Key, index, op.SequenceNumber, op.ClientId)

  if !isLeader || index <= 0 {
    reply.Err = ErrWrongLeader
    return 
  }

  ch := kv.getIndexCh(index)

  //wait agreement on the index command
  select {
  case applyOp := <- ch:
    if applyOp == op {
      // success commit command
      DPrintf(dGet, "Se%d T%d executed Op{Get %s} Index:%d from SN:%d Cli-%d", 
        kv.me, term, op.Key, index, op.SequenceNumber, op.ClientId)
      kv.mu.Lock()
      if value, exist := kv.data[op.Key]; exist {
        *reply = GetReply{Err: OK, Value:value}
      } else {
        reply.Err = ErrNoKey
      }
      kv.mu.Unlock()
    } else {
      reply.Err = ErrWrongLeader
      DPrintf(dTerm, "Se%d Lose leadership T%d not execute Op{Get %s} Index:%d from SN:%d Cli-%d", 
        kv.me, term, op.Key, index, op.SequenceNumber, op.ClientId)
    }
  case <-time.After(2 * electionTimeout):
    DPrintf(dGet, "Se%d times out waiting for Index: Op apply, SN:%d", kv.me, args.SequenceNumber, index)	
  }

  kv.mu.Lock()
  delete(kv.syncIndexChs, index)
  kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
  // Your code here.
  kv.mu.Lock()
  if kv.maxSeqNumberOfClients[args.ClientId] >= args.SequenceNumber { 
    reply.Err = OK
    kv.mu.Unlock()		
    return 
  } 
  kv.mu.Unlock()
  op := Op{Name: args.Op, Key: args.Key, Value: args.Value, 
    SequenceNumber: args.SequenceNumber, ClientId: args.ClientId}
  //wbuf := new(bytes.Buffer)
  //enc := labgob.NewEncoder(wbuf)
  //if enc.Encode(op) != nil {
  //	DFatalf("Op encode fail, unable to start cmd:%v", op)
  //}
  //cmd := wbuf.Bytes()
  index, term, isLeader := kv.rf.Start(op)
  DPrintf(dPut, "Se%d T%d Start Op{%s %s %s} Index:%d from SN:%d Cli-%d", 
  kv.me, term, op.Name, op.Key, op.Value, index, op.SequenceNumber, op.ClientId)

  if !isLeader || index <= 0 {
    reply.Err = ErrWrongLeader
    return
  }

  ch := kv.getIndexCh(index)

  //wait agreement on the index command
  select {
  case applyOp := <- ch:
    if applyOp == op {
      DPrintf(dPut, "Se%d T%d executed Op{%s %s %s} Index:%d from SN:%d Cli-%d", 
      kv.me, term, op.Name, op.Key, op.Value, index, op.SequenceNumber, op.ClientId)
      reply.Err = OK	
      // success commit command
    } else {
      reply.Err = ErrWrongLeader
      DPrintf(dTerm, "Se%d Lose leadership T%d, not execute Op{%s %s %s} Index:%d from SN:%d Cli-%d", 
        kv.me, term, op.Name, op.Key, op.Value, index, op.SequenceNumber, op.ClientId)
    }
  case <-time.After(2 * electionTimeout):
    DPrintf(dGet, "Se%d times out waiting for Index: Op apply, SN:%d", kv.me, args.SequenceNumber, index)	
  }
    
  kv.mu.Lock()
  delete(kv.syncIndexChs, index)
  kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
  atomic.StoreInt32(&kv.dead, 1)
  kv.rf.Kill()
  // Your code here, if desired.
}

func (kv *KVServer) killed() bool {
  z := atomic.LoadInt32(&kv.dead)
  return z == 1
}

func (kv *KVServer) applier() {
  for kv.killed() == false {
    var msg raft.ApplyMsg
    msg = <- kv.applyCh
    if msg.CommandValid {
      kv.mu.Lock()
      op, ok := msg.Command.(Op)
      if ok && op.Name != "Get" {
        if kv.maxSeqNumberOfClients[op.ClientId] < op.SequenceNumber {
          kv.executeWriteOpEL(&op)
          // DPrintf(dApply, "Se%d Index:%d Command:%v applied.", kv.me, msg.CommandIndex, op)
          kv.maxSeqNumberOfClients[op.ClientId] = op.SequenceNumber
        }
      }
      
      if _, isLeader := kv.rf.GetState(); isLeader {
        if ch, exist := kv.syncIndexChs[msg.CommandIndex]; exist {
          ch <- op
        }		
      }
      kv.mu.Unlock()
    }
  }
}

func (kv *KVServer) executeWriteOpEL(op *Op) {
  value, exist := kv.data[op.Key]
  if !exist || op.Name == "Put" {
    kv.data[op.Key] = op.Value
  } else if op.Name == "Append" {
    kv.data[op.Key] = value + op.Value
  }
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
  // call labgob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  labgob.Register(Op{})

  kv := new(KVServer)
  kv.me = me
  kv.maxraftstate = maxraftstate

  // You may need initialization code here.

  kv.applyCh = make(chan raft.ApplyMsg)
  kv.rf = raft.Make(servers, me, persister, kv.applyCh)

  // You may need initialization code here.
  kv.data = make(map[string]string)
  kv.maxSeqNumberOfClients = make(map[int64]int)
  kv.syncIndexChs = make(map[int]chan Op)
  go kv.applier()

  return kv
}
