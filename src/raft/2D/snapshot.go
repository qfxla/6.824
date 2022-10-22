package raft

import "6.824/labrpc"

type InstallSnapshotArgs struct {
	Term             int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}
	if lastIncludedIndex <= rf.size()-1 && rf.get(lastIncludedIndex).Term == lastIncludedTerm {
		rf.log = append([]LogEntry(nil), rf.log[lastIncludedIndex-rf.lastIncludeIndex:]...)
	} else {
		rf.log = append([]LogEntry(nil), LogEntry{Term: lastIncludedTerm})
	}

	rf.lastIncludeIndex = lastIncludedIndex
	rf.persister.snapshot = snapshot
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.persister.raftstate, rf.persister.snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if index <= rf.lastIncludeIndex {
		return
	}
	rf.log = rf.log[index-rf.lastIncludeIndex:]
	rf.lastIncludeIndex = index

	rf.persister.SaveStateAndSnapshot(rf.persister.raftstate, snapshot)
}

func (rf *Raft) size() int {
	return rf.lastIncludeIndex + len(rf.log)
}

func (rf *Raft) get(i int) LogEntry {
	return rf.log[i-rf.lastIncludeIndex]
}

func (rf *Raft) set(i int, e LogEntry) {
	rf.log[i-rf.lastIncludeIndex] = e
}

func (rf *Raft) sendSnapshot(id int, peer *labrpc.ClientEnd, args InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	ok := peer.Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.switchStateTo(Follower)
		return
	}
	rf.nextIndex[id] = args.LastIncludeIndex + 1
	//rf.nextIndex[id] = rf.nextIndex[id] - args.LastIncludeIndex
	rf.matchIndex[id] = args.LastIncludeIndex
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.switchStateTo(Follower)
		return
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIncludeIndex <= rf.lastIncludeIndex {
		return
	}
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludeIndex,
	}
}
