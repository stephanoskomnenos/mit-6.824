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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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

func (rf *Raft) sendApplyMsg() {
	rf.applyMu.Lock()
	defer rf.applyMu.Unlock()

	rf.mu.Lock()
	rf.lastApplied = min(rf.lastApplied, rf.commitIndex)
	lastApplied := rf.lastApplied
	logs := make([]LogEntry, rf.commitIndex-rf.lastApplied)
	copy(logs, rf.log[rf.lastApplied+1:rf.commitIndex+1])
	rf.lastApplied += len(logs)

	DPrintf("ApplyMsg: raft %d sending logs from index %d to %d, %v", rf.me, lastApplied+1, rf.commitIndex, logs)
	rf.mu.Unlock()

	for i, log := range logs {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      log.Command,
			CommandIndex: lastApplied + 1 + i,
		}

		DPrintf("ApplyMsg: raft %d, send log with index %d, %v", rf.me, lastApplied+1+i, log.Command)

		rf.applyCh <- applyMsg
	}
}

const (
	RfFollower = iota
	RfCandidate
	RfLeader
)

const (
	HeartbeatInterval    = 100 * time.Millisecond
	ElectionBaseInterval = 600 * time.Millisecond
)

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

	state int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// for test
	applyCh chan ApplyMsg

	applyMu sync.Mutex
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == RfLeader

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
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// 2B if args.PrevLogIndex > len(rf.log)-1, return this to reset nextIndex
	LastLogIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.LastLogIndex = -1

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("AppendEntries: raft %d rejects append entries due to term, current term %d", rf.me, rf.currentTerm)
		return
	}

	// 2B
	if len(rf.log)-1 < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		if len(rf.log)-1 < args.PrevLogIndex {
			reply.LastLogIndex = len(rf.log) - 1
			DPrintf("AppendEntries: raft %d rejects append entries due to log, len(rf.log)=%d, args.PrevLogIndex=%d", rf.me, len(rf.log), args.PrevLogIndex)
		} else {
			DPrintf("AppendEntries: raft %d rejects append entries due to log, rf.log[args.PrevLogIndex].Term=%d, args.PrevLogTerm=%d, current term %d", rf.me, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm, rf.currentTerm)
			rf.log = rf.log[:args.PrevLogIndex]
		}
		return
	}

	if args.Term > rf.currentTerm || rf.state != RfFollower {
		DPrintf("AppendEntries: raft %d update current term from %d to %d", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
	}

	// 2B
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	// DPrintf("AppendEntries: raft %d accepts append entries, commitIndex=%d, leaderCommit=%d", rf.me, rf.commitIndex, args.LeaderCommit)
	if len(args.Entries) > 0 {
		DPrintf("AppendEntries: raft %d accepts append entries, len(args.Entries)=%d, log %v", rf.me, len(args.Entries), rf.log)
	}

	// 2B
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		go rf.sendApplyMsg()
		DPrintf("AppendEntries: raft %d updates commitIndex to %d, %v", rf.me, rf.commitIndex, rf.log[:rf.commitIndex+1])
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func (rf *Raft) sendAppendEntries(follower int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[follower].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastHeartbeat() {

	for follower := range rf.peers {
		if follower == rf.me {
			continue
		}

		prevLogIndex := rf.nextIndex[follower] - 1
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  rf.log[prevLogIndex].Term,
			Entries:      rf.log[rf.nextIndex[follower]:],
			LeaderCommit: rf.commitIndex,
		}

		go func(follower int, prevLogIndex int) {
			reply := &AppendEntriesReply{}

			if !rf.sendAppendEntries(follower, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.convertTo(RfFollower)

				DPrintf("AppendEntries: failed, raft %d converts to follower", rf.me)

				return
			}

			if reply.Success {
				// 2B
				rf.matchIndex[follower] = prevLogIndex + len(args.Entries)
				rf.nextIndex[follower] = rf.matchIndex[follower] + 1

				// update commitIndex
				if rf.matchIndex[follower] > rf.commitIndex {
					count := 0
					for _, peerMatchIndex := range rf.matchIndex {
						if peerMatchIndex >= rf.matchIndex[follower] {
							count += 1
							// DPrintf("AppendEntries: leader %d, matchIndex[%d]=%d>=matchIndex[%d]=%d, count %d", rf.me, i, peerMatchIndex, follower, rf.matchIndex[follower], count)
						}
						if count > len(rf.peers)/2 {
							rf.commitIndex = rf.matchIndex[follower]
							go rf.sendApplyMsg()
							break
						}
					}
				}

				DPrintf("AppendEntries: success, commitIndex=%d, matchIndex[%d]=%d, nextIndex[%d]=%d", rf.commitIndex, follower, rf.matchIndex[follower], follower, rf.nextIndex[follower])

			} else {
				if reply.LastLogIndex != -1 {
					rf.nextIndex[follower] = reply.LastLogIndex + 1
				} else if rf.nextIndex[follower] >= 2 {
					rf.nextIndex[follower] -= 1
				}
				DPrintf("AppendEntries: failed, update nextIndex[%d] to %d", follower, rf.nextIndex[follower])
			}
		}(follower, prevLogIndex)
	}

	rf.heartbeatTimer.Reset(heartbeatDuration())
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// election finished
	if args.Term > rf.currentTerm {
		rf.convertTo(RfFollower)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		DPrintf("RequestVote: raft %d rejects %d's request, current term %d, request's term %d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	// 2B: add logs check
	// DPrintf("RequestVote: raft %d, args.LastLogTerm=%d, args.LastLogIndex=%d", rf.me, args.LastLogTerm, args.LastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)) {

		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionTimer.Reset(electionRandomDuration())

		DPrintf("RequestVote: raft %d accepts %d's request, current term %d", rf.me, args.CandidateId, rf.currentTerm)
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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

func (rf *Raft) startElection() {
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		// 2B
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.votedFor = rf.me
	voteCount := 1

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(peer, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.VoteGranted && rf.currentTerm == reply.Term {
				voteCount++
				if voteCount > len(rf.peers)/2 {
					// begin convertion to leader

					rf.convertTo(RfLeader)
					DPrintf("Election: %d becomes leader, current term %d", rf.me, rf.currentTerm)

					rf.matchIndex = make([]int, len(rf.peers))
					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i += 1 {
						if i == rf.me {
							rf.matchIndex[i] = len(rf.log) - 1
						} else {
							rf.matchIndex[i] = 0
						}
						rf.nextIndex[i] = len(rf.log)
					}

					// start broadcast heartbeat
					rf.broadcastHeartbeat()
				}
			} else if rf.currentTerm < reply.Term {
				rf.convertTo(RfFollower)
			}
		}(peer)
	}
}

func (rf *Raft) convertTo(state int) {
	switch state {
	case RfCandidate:
		rf.state = RfCandidate
	case RfFollower:
		rf.state = RfFollower
	case RfLeader:
		rf.state = RfLeader
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2B).

	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == RfLeader

	if isLeader {
		rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
		rf.matchIndex[rf.me] = len(rf.log) - 1
		rf.nextIndex[rf.me] = len(rf.log)
		rf.broadcastHeartbeat()

		DPrintf("Start: leader %d, index %d, %v", rf.me, index, rf.log[index])
	}

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.convertTo(RfCandidate)
			rf.currentTerm += 1
			rf.startElection()
			rf.electionTimer.Reset(electionRandomDuration())
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == RfLeader {
				rf.broadcastHeartbeat()
			}
			rf.mu.Unlock()
		}

	}
}

func heartbeatDuration() time.Duration {
	return HeartbeatInterval
}

func electionRandomDuration() time.Duration {
	rand.Seed(time.Now().UnixMicro())
	return ElectionBaseInterval + time.Millisecond*time.Duration(rand.Intn(200))
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
	rf.state = RfFollower
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.log = []LogEntry{LogEntry{Term: 0}}
	rf.heartbeatTimer = time.NewTimer(heartbeatDuration())
	rf.electionTimer = time.NewTimer(electionRandomDuration())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
