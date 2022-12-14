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
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
	"math/rand"
	"time"
	// "fmt"
)

type RaftState string

const (
	Follower           RaftState = "Follower"
	Candidate                    = "Candidate"
	Leader                       = "Leader"
	HEART_BEAT_TIMEOUT           = 100 //心跳超时，要求1秒10次，所以是100ms一次
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state         RaftState   // 状态属性，0 follower 1 candidate 2 leader
	electionTimer *time.Timer // 超时选举时间
	// heartBeat     time.Duration
	heartbeatTimer *time.Timer // 心跳定时器

	// term	int // 任期
	voteFor   int // 投票给谁
	voteCount int // 当前票数

	applyCh     chan ApplyMsg // 提交通道
	currentTerm int           //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int           //candidateId that received vote in current term (or null if none)
	log         []LogEntry    //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//Volatile state on all servers:
	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	//Volatile state on leaders:(Reinitialized after election)
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := (rf.state == Leader)
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	if isLeader {
		rf.log = append(rf.log, LogEntry{Command: command, Term: term})
		index = len(rf.log) - 1
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.votedFor = -1
	rf.heartbeatTimer = time.NewTimer(HEART_BEAT_TIMEOUT * time.Millisecond)
	rf.electionTimer = time.NewTimer(randTimeDuration())

	rf.applyCh = applyCh
	rf.log = make([]LogEntry, 1) // start from index 1

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	//以定时器的维度重写background逻辑
	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				switch rf.state {
				case Follower:
					rf.switchStateTo(Candidate)
				case Candidate:
					rf.leaderElection()
				}
				rf.mu.Unlock()

			case <-rf.heartbeatTimer.C:
				rf.mu.Lock()
				if rf.state == Leader {
					rf.appendEntries()
					rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT * time.Millisecond)
				}
				rf.mu.Unlock()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func randTimeDuration() time.Duration {
	return time.Duration(HEART_BEAT_TIMEOUT*3+rand.Intn(HEART_BEAT_TIMEOUT)) * time.Millisecond
}

// func (rf *Raft) resetElectionTimer() {
// 	t := time.Now()
// 	electedTime := time.Duration(150 + rand.Intn(150)) * time.Millisecond
// 	rf.electedTime = t.Add(electedTime)
// }

// 切换状态，调用者需要加锁
func (rf *Raft) switchStateTo(state RaftState) {
	if state == rf.state {
		return
	}
	DPrintf("Term %d: server %d convert from %v to %v\n", rf.currentTerm, rf.me, rf.state, state)
	rf.state = state
	switch state {
	case Follower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(randTimeDuration())
		rf.votedFor = -1

	case Candidate:
		//成为候选人后立马进行选举
		rf.leaderElection()

	case Leader:
		// initialized to leader last log index + 1
		for i := range rf.nextIndex {
			rf.nextIndex[i] = (len(rf.log))
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}

		rf.electionTimer.Stop()
		rf.appendEntries()
		rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT * time.Millisecond)
	}
}

func (rf *Raft) leaderElection() {
	rf.currentTerm += 1
	rf.votedFor = rf.me //vote for me
	rf.voteCount = 1
	rf.electionTimer.Reset(randTimeDuration())

	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log[lastLogIndex].Term,
	}
	for i := range rf.peers {
		if i != rf.me {
			go func(peer int) {
				var reply RequestVoteReply
				if rf.sendRequestVote(peer, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.switchStateTo(Follower)
					}
					if reply.VoteGranted && rf.state == Candidate {
						rf.voteCount++
						if rf.voteCount > len(rf.peers)/2 {
							rf.switchStateTo(Leader)
						}
					}
				}
			}(i)
		}
	}

}

// func (rf *Raft) candidateRequestVote(serverId int, args *RequestVoteArgs, voteCounte *int, becomeLeader *sync.Once) {
// 	reply := RequestVoteReply{}

// 	if rf.sendRequestVote(serverId, args, & reply) {
// 		rf.mu.Lock()
// 		defer rf.mu.Unlock()
// 		if reply.Term > args.Term {
// 			rf.currentTerm = reply.Term
//             rf.switchStateTo(Follower)
// 		}
// 		if reply.Term < args.Term {
// 			return
// 		}

// 		if reply.VoteGranted && rf.state == Candidate {
// 			*voteCounte++
// 			if *voteCounte > len(rf.peers) / 2 {
// 				becomeLeader.Do(func() {
// 					rf.state = Leader
// 				})
// 			}
// 		}
// 	}
// }

func min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}
