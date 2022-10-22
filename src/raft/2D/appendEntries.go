package raft

import "fmt"

type AppendEntriesArgs struct {
	Term         int        //leader’s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader’s commitIndex
}
type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) appendEntries() {
	for peer, _ := range rf.peers {
		if peer != rf.me {
			go rf.leaderSendEntries(peer)
		}
	}
}

func (rf *Raft) leaderSendEntries(serverId int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[serverId] - 1

	if prevLogIndex <= rf.lastIncludeIndex {
		go rf.sendSnapshot(serverId, rf.peers[serverId], InstallSnapshotArgs{
			Term:             rf.currentTerm,
			LastIncludeIndex: rf.lastIncludeIndex,
			LastIncludeTerm:  rf.log[0].Term,
			Data:             rf.persister.snapshot,
		})
	}

	// use deep copy to avoid race condition
	// when override log in AppendEntries()
	fmt.Println("rf- ", rf.me, " len(log):", len(rf.log), " , index0:", rf.lastIncludeIndex, "prevLogIndex:", prevLogIndex)
	entries := make([]LogEntry, len(rf.log[(prevLogIndex+1):]))
	copy(entries, rf.log[(prevLogIndex+1):])
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[(prevLogIndex)].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	var reply AppendEntriesReply
	if rf.sendAppendEntries(serverId, &args, &reply) {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		// If last log index ≥ nextIndex for a follower: send
		// AppendEntries RPC with log entries starting at nextIndex
		// • If successful: update nextIndex and matchIndex for
		// follower (§5.3)
		// • If AppendEntries fails because of log inconsistency:
		// decrement nextIndex and retry (§5.3)
		if reply.Success {
			rf.matchIndex[serverId] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[serverId] = rf.matchIndex[serverId] + 1

			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			// set commitIndex = N (§5.3, §5.4).
			for N := (len(rf.log) - 1); N > rf.commitIndex; N-- {
				count := 0
				for _, matchIndex := range rf.matchIndex {
					if matchIndex >= N {
						count += 1
					}
				}

				if count > len(rf.peers)/2 {
					// most of nodes agreed on rf.log[i]
					rf.setCommitIndex(N)
					break
				}
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
			} else { // 对应Append的2、3
				// 如果走到这个这里，那就往前推，找到彼此匹配的index
				//rf.nextIndex[serverId] = args.PrevLogIndex - 1
				rf.nextIndex[serverId] = reply.ConflictIndex

				if reply.ConflictTerm != -1 {
					for i := args.PrevLogIndex; i >= 1; i-- {
						if rf.log[i-1].Term == reply.ConflictTerm {
							rf.nextIndex[serverId] = i
							break
						}
					}
				}
			}
		}
		rf.mu.Unlock()
	}
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = true
	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// If RPC request or response contains term T > term:set term = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.switchStateTo(Follower)
	}
	// reset election timer even log does not match
	// args.LeaderId is the current term s Leader
	rf.electionTimer.Reset(randTimeDuration())

	// 2. Reply false if log does not contain an entry at prevLogIndex
	// whose term matches prevLogTerm (5.3)
	// args.PrevLogIndex一开始最大是Leader的len(log)+1,如果比从机大，那就-1 或后期len
	lastLogIndex := len(rf.log) - 1
	if lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm

		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.log)
		return
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but diffent term), delete the existing entry and all that
	// follow it (5.3)
	// 日志长度能匹配上了，但是对应的term不一样，那也不行
	if rf.log[(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		reply.ConflictIndex = -1
		return
	}

	// 4. Append any new entries not already in the log
	// compare from rf.log[args.PrevLogIndex + 1]
	unmatch_idx := -1
	for idx := range args.Entries {
		if len(rf.log) < (args.PrevLogIndex+2+idx) ||
			rf.log[(args.PrevLogIndex+1+idx)].Term != args.Entries[idx].Term {
			// unmatch log found
			unmatch_idx = idx
			break
		}
	}

	if unmatch_idx != -1 {
		// there are unmatch entries
		// truncate unmatch Follwer entries, and apply Leader entries
		rf.log = rf.log[:(args.PrevLogIndex + 1 + unmatch_idx)]
		rf.log = append(rf.log, args.Entries[unmatch_idx:]...)
	}

	// 5. If LeaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.setCommitIndex(min(args.LeaderCommit, len(rf.log)-1))
	}

	reply.Success = true
	rf.persist()
}

// serveral setters, should be called with a lock
func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.commitIndex = commitIndex
	// apply all entries between lastApplied and committed
	// should be called after commitIndex updated
	if rf.commitIndex > rf.lastApplied {
		entriesToApply := append([]LogEntry{}, rf.log[(rf.lastApplied+1):(rf.commitIndex+1)]...)

		go func(startIdx int, entries []LogEntry) {
			for idx, entry := range entries {
				var msg ApplyMsg
				msg.CommandValid = true
				msg.Command = entry.Command
				msg.CommandIndex = startIdx + idx
				rf.applyCh <- msg
				// do not forget to update lastApplied index
				// this is another goroutine, so pretect it with lock
				rf.mu.Lock()
				if rf.lastApplied < msg.CommandIndex {
					rf.lastApplied = msg.CommandIndex
				}
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, entriesToApply)
	}
}
