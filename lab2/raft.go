package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
  "bytes"
  "sync"
  "sync/atomic"
  "math/rand"
  "time"
  "6.824/labgob"
  "6.824/labrpc"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
  CommandValid bool
  Command      interface{}
  CommandIndex int

  // For 2D:
  SnapshotValid bool
  Snapshot      []byte
  SnapshotTerm  int
  SnapshotIndex int
}

type RoleType int

const (
  Follower RoleType = iota
  Candidate 
  Leader
)

type Entry struct {
  Term	int
  Command	interface{}
}

type Log struct {
  findex	int
  entries []Entry
}

func (log *Log) firstIndex() int {
  return log.findex
}

func (log *Log) lastIndex() int {
  return log.findex + len(log.entries) - 1
}

func (log *Log) entry(index int) Entry {
  return log.entries[index-log.findex]
}

func (log *Log) allEntry() []Entry {
  return log.entries
}

func (log *Log) slice(start int) []Entry {
  return log.entries[start-log.findex:]
}

func (log *Log) append(e ...Entry) {
  log.entries = append(log.entries, e...)
}

func (log *Log) compaction(index int) {
  retainEntries := make([]Entry, len(log.entries)+log.findex-index)
  copy(retainEntries, log.entries[index-log.findex:])
  log.entries = retainEntries
  log.findex = index
}

func (log *Log) truncate(index int) {
  log.entries = log.entries[0:index-log.findex]
}

func (log *Log) restore(index int, entries []Entry) {
  log.findex = index
  log.entries = entries
}

func initLog() Log {
  log := Log{}
  log.findex = 0
  log.entries = make([]Entry, 1)
  return log
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
  mu        sync.Mutex          // Lock to protect shared access to this peer's state
  peers     []*labrpc.ClientEnd // RPC end points of all peers
  persister *Persister          // Object to hold this peer's persisted state
  me        int                 // this peer's index into peers[]
  dead      int32               // set by Kill()

  // Your data here (2A, 2B, 2C).
  // Look at the paper's Figure 2 for a description of what
  // state a Raft server must maintain.
  
  // Persistent state
  currentTerm	int
  votedFor	int
  log 	    Log

  // Volatile state
  commitIndex int				// index of highest log entry known to be committed
  lastApplied	int				// index of highest log entry applied to state machine
  state       RoleType		// the role of the server 
  lastTime    time.Time		// the start time of election timer
  appCond		*sync.Cond		// sync apply loop

  applyCh		chan ApplyMsg	// send commited log to app
  
  // Volatile state, only leader update
  nextIndex  []int 			// for each server, index of the next log entry to send to that server
  matchIndex []int			// for each server, index of highest log entry known to be replicated on server

  // Snapshot state
  snapshot 	  []byte
  snapshotIndex int
  snapshotTerm  int
  syncSnapshot  bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

  var term int
  var isleader bool
  // Your code here (2A).
  rf.mu.Lock()
  term = rf.currentTerm
  isleader = (rf.state == Leader)
  rf.mu.Unlock()
  return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
  // Your code here (2C).
  // Example:
  // w := new(bytes.Buffer)
  // e := labgob.NewEncoder(w)
  // e.Encode(rf.xxx)
  // e.Encode(rf.yyy)
  // data := w.Bytes()
  // rf.persister.SaveRaftState(data)
   wbuf := new(bytes.Buffer)
  enc := labgob.NewEncoder(wbuf)
  if enc.Encode(rf.currentTerm) != nil || 
     enc.Encode(rf.votedFor) != nil ||
     enc.Encode(rf.snapshotTerm) != nil {
    Debug(dError, "S%d Failed to save state T:%d VF:%d LIT:%d", rf.me, rf.currentTerm, rf.votedFor, rf.snapshotTerm)
  } else {
    Debug(dPersist, "S%d Saved State T:%d VF:%d LIT:%d", rf.me, rf.currentTerm, rf.votedFor, rf.snapshotTerm)
  }
  if enc.Encode(rf.log.firstIndex()) != nil || 
     enc.Encode(rf.log.allEntry()) != nil {
    Debug(dError, "S%d Failed to save log (%d, %d)", rf.me, rf.log.firstIndex()-1, rf.log.firstIndex())
  } else {
    Debug(dLog2, "S%d Saved Log (%d, %d) %v", rf.me, rf.log.firstIndex()-1, rf.log.firstIndex(), rf.log.allEntry())
  }
  data := wbuf.Bytes()
  rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
  if data == nil || len(data) < 1 { // bootstrap without any state?
    return
  }
  // Your code here (2C).
  // Example:
  // r := bytes.NewBuffer(data)
  // d := labgob.NewDecoder(r)
  // var xxx
  // var yyyb
  // if d.Decode(&xxx) != nil ||
  //    d.Decode(&yyy) != nil {
  //   error...
  // } else {
  //   rf.xxx = xxx
  //   rf.yyy = yyy
  // }
  
  rbuf := bytes.NewBuffer(data)
  dec := labgob.NewDecoder(rbuf)
  var currentTerm int
  var votedFor int
  var snapshotTerm int
  var entries []Entry
  var index int
  if dec.Decode(&currentTerm) != nil || 
     dec.Decode(&votedFor) != nil || 
     dec.Decode(&snapshotTerm) != nil {
    Debug(dError, "S%d Failed to restore state.")
  } else {
    rf.currentTerm = currentTerm
    rf.votedFor = votedFor
    rf.snapshotTerm = snapshotTerm
    Debug(dPersist, "S%d Restored State T:%d VF:%d LIT:%d", rf.me, rf.currentTerm, rf.votedFor, rf.snapshotTerm)
  }
  if dec.Decode(&index) != nil || 
     dec.Decode(&entries) != nil {
    Debug(dError, "S%d Failed to restore log (%d, %d)", rf.me, rf.log.firstIndex()-1, rf.log.firstIndex())		
  } else {
    rf.snapshotIndex = index - 1
    rf.log.restore(index, entries)
    Debug(dLog2, "S%d Restored log (%d, %d) %v", rf.me, rf.log.firstIndex()-1, rf.log.firstIndex(), rf.log.allEntry())
  }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

  // Your code here (2D).

  return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
  // Your code here (2D).
  rf.mu.Lock()
  if  rf.snapshotIndex < index {
    rf.snapshotIndex = index
    rf.snapshotTerm = rf.log.entry(index).Term
    rf.snapshot = snapshot
    rf.log.compaction(index+1)
  }
  rf.mu.Unlock()
}

func (rf *Raft) readSnapshot(data []byte) {
  rf.snapshot = data
}

type InstallSnapshotArgs struct {
  Term 	 		  int	 // leader’s term
  LeaderId 		  int 	 // so follower can redirect clients
  LastIncludedIndex int 	 // the snapshot replaces all entries up through and including this index
  LastIncludedTerm  int 	 // term of lastIncludedIndex
  Data 			  []byte // raw bytes of the snapshot
}

type InstallSnapshotReply struct {
  Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
  rf.mu.Lock()
  if args.Term > rf.currentTerm {
    rf.setNewTermEL(args.Term)
  }
  if !(args.Term < rf.currentTerm) {
    if rf.snapshotIndex < args.LastIncludedIndex {
      rf.snapshot = args.Data
      rf.snapshotIndex = args.LastIncludedIndex 
      rf.snapshotTerm = args.LastIncludedTerm
      rf.log.compaction(rf.snapshotIndex+1)
      rf.syncSnapshot = true
    }
  }
  reply.Term = rf.currentTerm
  rf.mu.Unlock()
  return nil
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
  ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
  return ok
}

func (rf *Raft) sendSnapshotEL(peer int) {
  args := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.snapshotIndex, rf.snapshotTerm, rf.snapshot}
  reply := InstallSnapshotReply{}
  ok := rf.sendInstallSnapshot(peer, &args, &reply)
  if ok {
    if reply.Term > rf.currentTerm {
      rf.setNewTermEL(reply.Term)
    }
  }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
  // Your data here (2A, 2B).
  Term 		 int	// candidate’s term
  CandidateId  int	// candidate requesting vote
  LastLogIndex int	// index of candidate’s last log entry
  LastLogTerm  int	// term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
  // Your data here (2A).
  Term 		int		// currentTerm, for candidate to update itself
  VoteGranted bool	// true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
  // Your code here (2A, 2B).
  rf.mu.Lock()
  if args.Term > rf.currentTerm {
    rf.setNewTermEL(args.Term)
  }
  Debug(dVote, "S%d C%d asking for vote, T%d", rf.me, args.CandidateId, rf.currentTerm)
  if args.Term < rf.currentTerm {
    reply.VoteGranted = false
  } else {
    // up-to-date restriction
    meLastIndex := rf.log.lastIndex() 
    meLastTerm := rf.log.entry(meLastIndex).Term
    meUpToDate := meLastTerm > args.LastLogTerm || (meLastTerm == args.LastLogTerm && meLastIndex > args.LastLogIndex) 
    if (!meUpToDate && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)) {
      rf.votedFor = args.CandidateId
      rf.persist()
      reply.VoteGranted = true
      rf.lastTime = time.Now()
      Debug(dVote, "S%d Granting Vote to S%d at T%d", rf.me, rf.votedFor, rf.currentTerm)
    } else {
      reply.VoteGranted = false
    }
  }
  reply.Term = rf.currentTerm
  rf.mu.Unlock()
  return nil
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
  ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  return ok
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
  Term 		 int 		// leader’s term
  LeaderId 	 int 		// so follower can redirect clients
  PrevLogIndex int 		// index of log entry immediately preceding new ones
  PrevLogTerm  int		// term of prevLogIndex entry
  Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
  LeaderCommit int		// leader’s commitIndex
}

type AppendEntriesReply struct {
  Term    int 	// currentTerm, for leader to update itself
  Success bool 	// if follower contained entry matching prevLogIndex and prevLogTerm
  XTerm   int		// term of conflicting entry
  XIndex	int		// first index of conflicting entry
}

//
// appendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
  rf.mu.Lock()
  defer rf.mu.Unlock()
  if args.Term > rf.currentTerm {
    rf.setNewTermEL(args.Term)
  }
  if args.Term < rf.currentTerm {
    reply.Success = false		
  } else {
    Debug(dTimer, "S%d Resetting ELT, from C%d received AppEnt T%d", rf.me, args.LeaderId, args.Term)
    rf.lastTime = time.Now()

    if args.PrevLogIndex > rf.log.lastIndex() {
      *reply = AppendEntriesReply{ Success: false, XTerm: -1, XIndex: rf.log.lastIndex()+1 }
    } else if rf.log.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
      reply.Success = false
      reply.XTerm = rf.log.entry(args.PrevLogIndex).Term
      for firstIndex := args.PrevLogIndex; firstIndex > 0; firstIndex-- {
        if rf.log.entry(firstIndex).Term != reply.XTerm {
          break
        }
        reply.XIndex = firstIndex
      }
    } else {
      //  truncate and append new log entries
      for offset := 1; offset < len(args.Entries)+1; offset++ {
        index := args.PrevLogIndex + offset
        if index > rf.log.lastIndex() || rf.log.entry(index).Term != args.Entries[offset-1].Term {
          rf.log.truncate(index)
          Debug(dLog, "S%d Remove Log, conflict index:%d from %v", rf.me, index, rf.log.allEntry())
          rf.log.append(args.Entries[offset-1:]...)
          Debug(dLog, "S%d Replicate Log, start from index:%d result %v", rf.me, index, rf.log.allEntry())
          rf.persist()
          break
        }
      }
      if (args.LeaderCommit > rf.commitIndex) {
        rf.commitIndex = min(args.LeaderCommit, rf.log.lastIndex())
      }
      reply.Success = true
      rf.appCond.Broadcast()
    }
  }
  reply.Term = rf.currentTerm
  return nil
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
  ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
  return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
  index := -1
  term := -1
  isLeader := true

  // Your code here (2B).
  rf.mu.Lock()
  term = rf.currentTerm
  if rf.state == Leader && rf.killed() == false {
    index = rf.log.lastIndex() + 1
    isLeader = true
    rf.log.append(Entry{rf.currentTerm, command})
    rf.persist()
    Debug(dClient, "S%d Append one new log entry - %v, start agreement.", rf.me, rf.log.entry(index))
    rf.nextIndex[rf.me] = index + 1
    rf.matchIndex[rf.me] = index
    rf.sendAppendBroadcastEL(false)
  }
  rf.mu.Unlock()
  return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
  atomic.StoreInt32(&rf.dead, 1)
  // Your code here, if desired.
  rf.appCond.Broadcast()
}

func (rf *Raft) killed() bool {
  z := atomic.LoadInt32(&rf.dead)
  return z == 1
}

// must check term of commit log: log[N].term == currentTerm
func (rf *Raft) commitLogEL() {
  if rf.state == Leader {
    for index := rf.commitIndex+1; index < rf.log.lastIndex()+1; index++ {
      if  rf.log.entry(index).Term != rf.currentTerm {
        continue
      }
      nmatch := 0
      for i := range(rf.peers) {
        if (rf.matchIndex[i] >= index) { nmatch += 1 }
      }
      if nmatch > len(rf.peers) / 2 {
        rf.commitIndex = index
        Debug(dCommit, "S%d Commit log, last committed index:%d", rf.me, rf.commitIndex)
      }
    }
    rf.appCond.Broadcast()
  }
}

func (rf *Raft) sendAppend(peer int, args *AppendEntriesArgs) {
  Debug(dLog, "S%d -> S%d Sending PLI: %d PLT: %d N: %d LC: %d - %v", 
    rf.me, peer, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit, args.Entries)
  
  reply := AppendEntriesReply{}
  ok := rf.sendAppendEntries(peer, args, &reply)
  if ok {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.currentTerm < reply.Term {
      rf.setNewTermEL(reply.Term)
    }
    if rf.currentTerm == args.Term {
      if reply.Success {
        // update nextIndex and matchIndex
        newNext := args.PrevLogIndex + 1 + len(args.Entries)
        newMatch := args.PrevLogIndex + len(args.Entries)
        if newNext > rf.nextIndex[peer] { rf.nextIndex[peer] = newNext }
        if newMatch > rf.matchIndex[peer] { rf.matchIndex[peer] = newMatch }
        Debug(dLog, "S%d <- S%d OK Append MI: %d", rf.me, peer, rf.matchIndex[peer])
      } else if rf.nextIndex[peer] > 1 {
        Debug(dLog, "S%d <- S%d Conflict Term: %d, First Index: %d", rf.me, peer, reply.XTerm, reply.XIndex)
        if reply.XTerm == -1 || reply.XTerm > rf.currentTerm {
          rf.nextIndex[peer] = reply.XIndex
        } else if {
          rf.sendSnapshotEL(peer)
        } else {
          for {
            rf.nextIndex[peer] -= 1
            if rf.nextIndex[peer] <= 1 { 
              break
            }
            if rf.log.entry(rf.nextIndex[peer]).Term == reply.XTerm {
              //rf.nextIndex[peer] += 1
              break
            }
            if rf.log.entry(rf.nextIndex[peer]).Term < reply.XTerm {
              rf.nextIndex[peer] = reply.XIndex
              break
            }
          }
        }
      }
    }
    rf.commitLogEL()
  }
}

// send append to all servers  
func (rf *Raft) sendAppendBroadcastEL(isHeartbeat bool) {
  if isHeartbeat {
    Debug(dTimer, "S%d Broadcast, resetting HBT.", rf.me)
  } else {
    Debug(dTimer, "S%d Broadcast, append log entries.", rf.me)
  }
  
  for peer := range rf.peers {
    if peer != rf.me {
      if isHeartbeat || rf.log.lastIndex() > rf.nextIndex[peer] {
        var prevIndex int
        var prevTerm int
         if rf.nextIndex[peer] > rf.log.firstIndex() {
          prevIndex := rf.nextIndex[peer] - 1
          prevTerm :=  rf.log.entry(prevIndex).Term
        } else {
          prevIndex := rf.snapshotIndex
          prevTerm :=  rf.snapshotTerm
        }
        entries := make([]Entry, rf.log.lastIndex()-prevIndex)
        copy(entries, rf.log.slice(prevIndex+1))
        args := AppendEntriesArgs{rf.currentTerm, rf.me, prevIndex, prevTerm, 
          entries, rf.commitIndex}
        go rf.sendAppend(peer, &args)
      }
    }
  }
}

func (rf *Raft) becomeNewLeaderEL() {
  rf.state = Leader
  rf.persist()
  for i := range(rf.nextIndex) {
    rf.nextIndex[i] = rf.log.lastIndex()+1
  }
  for i := range(rf.matchIndex) {
    rf.matchIndex[i] = 0
  }
  rf.matchIndex[rf.me] = rf.log.lastIndex()
  Debug(dTimer, "S%d Leader, checking heartbeats T:%d", rf.me, rf.currentTerm)
  go rf.heartbeats()
}

func (rf *Raft) setNewTermEL(term int) {
  Debug(dTerm, "S%d Term is higher, updating %d > %d", rf.me, term, rf.currentTerm)
  rf.state = Follower
  rf.currentTerm = term
  rf.votedFor = -1
  rf.persist()
}

func (rf *Raft) startRequestVote(peer int, votes *int, args *RequestVoteArgs) {		
  reply := RequestVoteReply{}
  ok := rf.sendRequestVote(peer, args, &reply)
  if ok {
    rf.mu.Lock()
    if reply.Term > rf.currentTerm {
      rf.setNewTermEL(reply.Term)
    }
    // 不需要判断转状态是不是候选者，因为只要任期变了，状态一定也就变了
    if rf.currentTerm == args.Term {
      if reply.VoteGranted {
        Debug(dVote, "S%d <- S%d Got Vote", rf.me, peer)
        *votes += 1
        if *votes > len(rf.peers)/2 && rf.state != Leader {
          Debug(dLeader, "S%d Achieved Majority for T%d (v:%d)", rf.me, rf.currentTerm, *votes)
          rf.becomeNewLeaderEL()
          rf.sendAppendBroadcastEL(true)
        }
      }
    }
    rf.mu.Unlock()
  }
}

// start leader election
func (rf *Raft) leaderElectionEL() {
  rf.currentTerm += 1
  rf.votedFor = rf.me
  rf.state = Candidate
  rf.persist()
  Debug(dTimer, "S%d Converting to Candidate, calling election T:%d", rf.me, rf.currentTerm)

  votes := 1
  for peer := range rf.peers {
    if peer != rf.me {
      lastIndex := rf.log.lastIndex()
      args := RequestVoteArgs{rf.currentTerm, rf.me, lastIndex, rf.log.entry(lastIndex).Term} 
      go rf.startRequestVote(peer, &votes, &args)
    }
  }
}

func (rf *Raft) applier() {
  for rf.killed() == false {
    rf.mu.Lock()
    if rf.syncSnapshot {
      applyMsg := ApplyMsg{}
      applyMsg.SnapshotValid = true
      applyMsg.Snapshot = rf.snapshot
      applyMsg.SnapshotTerm = rf.snapshotTerm
      applyMsg.SnapshotIndex =rf.snapshotIndex
      rf.syncSnapshot = false
      rf.mu.Unlock()
      rf.applyCh <- applyMsg
    } else if rf.lastApplied < rf.commitIndex {
      rf.lastApplied += 1
      committedLog := rf.log.entry(rf.lastApplied)
      applyMsg := ApplyMsg{}
      applyMsg.CommandValid = true
      applyMsg.Command = committedLog.Command 
      applyMsg.CommandIndex = rf.lastApplied
      rf.mu.Unlock()
      Debug(dCommit, "S%d Sending one command to applyCh, LA:%d CMD:%v", rf.me, rf.lastApplied, committedLog.Command)
      rf.applyCh <- applyMsg
    } else {
      //Debug(dCommit, "S%d Nothing left to apply await LA:%d CI:%v", rf.me, rf.lastApplied, rf.commitIndex)
      rf.appCond.Wait()
      rf.mu.Unlock()
    }
  }
}

func (rf *Raft) heartbeats() {
  for rf.killed() == false {
    rf.mu.Lock()
    rf.lastTime = time.Now()
    if rf.state == Leader {
      rf.sendAppendBroadcastEL(true)
    } else {
      Debug(dTimer, "S%d Not Leader, checking election timeout", rf.me)	
      if rf.state == Follower {
        Debug(dTimer, "S%d I'm follower, pausing HBT", rf.me)
      }
      rf.mu.Unlock()
      break
    }
    rf.mu.Unlock()
    time.Sleep(100 * time.Millisecond)
  }
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
  randomTimeout := func(a int64, b int64) time.Duration {
    ms := a + rand.Int63() % (b-a)
    return time.Duration(ms) * time.Millisecond
  }

  electionTimeout := randomTimeout(500, 1000)
  for rf.killed() == false {

    // Your code here to check if a leader election should
    // be started and to randomize sleeping time using
    // time.Sleep().
    rf.mu.Lock()
    if time.Since(rf.lastTime) > electionTimeout {
      Debug(dTimer, "S%d Resetting ELT because election", rf.me)
      electionTimeout = randomTimeout(500, 1000)
      rf.lastTime = time.Now()
      rf.leaderElectionEL()
    }
    rf.mu.Unlock()
    time.Sleep(50 * time.Millisecond)
  }
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
  persister *Persister, applyCh chan ApplyMsg) *Raft {
  rf := &Raft{}
  rf.peers = peers
  rf.persister = persister
  rf.me = me

  // Your initialization code here (2A, 2B, 2C).
  rf.votedFor = -1
  rf.state = Follower
  rf.currentTerm = 0
  rf.lastTime = time.Now()
  
  rf.commitIndex = 0
  
  rf.log = initLog()

  rf.lastApplied = 0
  rf.appCond = sync.NewCond(&rf.mu)
  rf.applyCh = applyCh

  rf.nextIndex = make([]int, len(rf.peers)) 
  rf.matchIndex = make([]int, len(rf.peers))
  
  rand.Seed(time.Now().UnixNano())

  // initialize from state persisted before a crash
  rf.readPersist(persister.ReadRaftState())
  rf.readSnapshot(persister.ReadSnapshot())
  
  Debug(dClient, "S%d Start at T:%d", rf.me, rf.currentTerm)
  // start ticker goroutine to start elections
  go rf.ticker()
  // start heartsbeats goroutine to send AppendEntries periodically
  go rf.heartbeats()
  // apply goroutine
  go rf.applier()
  return rf
}
