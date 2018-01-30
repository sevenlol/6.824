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
	"fmt"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type logEntry struct {
	Term    int
	Command interface{}
}

type peerRole int

const (
	follower peerRole = iota
	leader
	candidate
)

const (
	minElectionTimeout = 750
	maxElectionTimeout = 950
	heartBeatInterval  = 120
	rpcTimeout         = 5000
	startTimeout       = 5000

	electionTimerName  = "election"
	heartBeatTimerName = "heartbeat"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	c         *sync.Cond          // Condition variable to signal commitIndex change for Start()
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/* persisted */
	currentTerm int
	votedFor    *int
	log         []*logEntry
	// indicate the "real" size of log
	logCount int

	/* volatile */
	role        peerRole
	commitIndex int
	lastApplied int

	/* leader */
	nextIndex  []int
	matchIndex []int

	/* other */
	electionTimerCh chan bool
	appendEntriesCh chan bool
	// indicate commitIndex changed to checking goroutine
	commitCh chan bool
	// indicate replication status (matchIndex) change
	replicationCh chan bool
	quit          chan bool
	applyCh       chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool

	// Your code here (2A).

	term = rf.currentTerm
	isleader = (rf.role == leader)

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
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.log[:rf.logCount])
	if rf.votedFor == nil {
		enc.Encode(-1)
	} else {
		enc.Encode(*rf.votedFor)
	}
	enc.Encode(rf.role)

	// debug(rf.me, rf.currentTerm, "save state, logCount=%d\n", rf.logCount)

	data := buf.Bytes()
	rf.persister.SaveRaftState(data)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	var term int
	var log []*logEntry
	var votedFor int
	var role peerRole
	if dec.Decode(&term) != nil || dec.Decode(&log) != nil ||
		dec.Decode(&votedFor) != nil || dec.Decode(&role) != nil {
		rf.debug("failed to read raft state\n")
	} else {
		debug(rf.me, rf.currentTerm, "read state, logCount=%d\n", len(log))
		rf.currentTerm = term
		rf.log = log
		rf.logCount = len(log)
		if votedFor == -1 {
			rf.votedFor = nil
		} else {
			rf.votedFor = &votedFor
		}
		rf.role = role
		if rf.role == leader {
			rf.initializeLeaderState()
		}
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
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

// AppendEntriesArgs RPC arguments structure for heartbeat
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*logEntry
	LeaderCommit int
}

// AppendEntriesReply RPC reply structure
type AppendEntriesReply struct {
	Term         int
	Success      bool
	ConflictTerm int
	// first index of conflict term
	FirstLogIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		debug(rf.me, rf.currentTerm, "reject voting req with lower term=%d\n", args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		if rf.role == leader {
			rf.electionTimerCh <- true
		}
		rf.currentTerm = args.Term
		rf.role = follower
		rf.votedFor = nil
		reply.Term = args.Term
		rf.persist()
	}
	if rf.votedFor != nil && *rf.votedFor != args.CandidateID {
		// same term, already vote for others
		debug(rf.me, rf.currentTerm, "already vote for %d, rejecting voting req\n", *rf.votedFor)
		return
	}
	// peer's log is not as up to date as this server's, reject
	if rf.logCount != 0 && (args.LastLogTerm < rf.log[rf.logCount-1].Term ||
		(args.LastLogTerm == rf.log[rf.logCount-1].Term && args.LastLogIndex < rf.logCount)) {
		debug(rf.me, rf.currentTerm, "candidate's log is older, rejecting vote request=%v,"+
			"lastIndex=%d, lastTerm=%d, role=%d\n", args, rf.logCount, rf.log[rf.logCount-1].Term, rf.role)
		rf.c.Broadcast()
		return
	}

	// 1. not voting for anyone yet
	// 2. vote for someone but this request has higher term

	reply.VoteGranted = true
	// vote for this request
	id := args.CandidateID
	rf.votedFor = &id
	// reset election timer
	rf.electionTimerCh <- true
	rf.persist()

	debug(rf.me, rf.currentTerm, "vote for server=%d, lastIndex=%d\n", args.CandidateID, rf.logCount)
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	debug(rf.me, rf.currentTerm, "receives AppendEntries=%v, logCount=%d\n", args, rf.logCount)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.ConflictTerm = 0
		reply.FirstLogIndex = rf.logCount + 1
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = follower
		rf.votedFor = nil
		reply.Term = args.Term
	}

	leaderID := args.LeaderID

	// reset timer (higher term or current leader)
	if rf.votedFor == nil || *rf.votedFor == args.LeaderID {
		rf.electionTimerCh <- true
	}

	// accept leader request and change to follower
	rf.currentTerm = args.Term
	rf.votedFor = &leaderID
	rf.role = follower
	rf.persist()

	// configure reply
	reply.Success = true

	// no PrevLogIndex in log
	if args.PrevLogIndex != 0 && rf.logCount < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictTerm = 0
		reply.FirstLogIndex = rf.logCount + 1
		debug(rf.me, rf.currentTerm, "reject AppendEntries request=%v, logCount=%d\n", args, rf.logCount)
		return
	}
	// prevLog from args not match (term not match)
	if args.PrevLogIndex != 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		// reject
		reply.Success = false
		// set the conflict term and the first log index of that term
		reply.ConflictTerm = rf.log[args.PrevLogIndex-1].Term
		reply.FirstLogIndex = args.PrevLogIndex
		for i := 1; i <= rf.logCount; i++ {
			if rf.log[i-1].Term == reply.ConflictTerm {
				reply.FirstLogIndex = i
				break
			}
		}
		/* for i := args.PrevLogIndex - 1; i > 0; i-- {
			reply.FirstLogIndex = i + 1
			if rf.log[i-1].Term != rf.log[i].Term {
				break
			}
		} */
		debug(rf.me, rf.currentTerm,
			"reject AppendEntries request=%v, logCount=%d, conflictTerm=%d, conflictIndex=%d\n",
			args, rf.logCount, reply.ConflictTerm, reply.FirstLogIndex)
		return
	}

	// store entries from args and remove all logs after entries
	if args.Entries != nil && len(args.Entries) > 0 {
		// start slice index
		start := args.PrevLogIndex
		haveAll := true
		for i := 0; i < len(args.Entries); i++ {
			if i+start < len(rf.log) {
				if rf.log[i+start].Term != args.Entries[i].Term {
					haveAll = false
				}
				rf.log[i+start] = args.Entries[i]
			} else {
				haveAll = false
				rf.log = append(rf.log, args.Entries[i])
			}
		}
		if !haveAll {
			// truncate the log if this peer does not already have all the
			// entries leader sent
			rf.logCount = args.PrevLogIndex + len(args.Entries)
		}
		rf.persist()
		debug(rf.me, rf.currentTerm, "update log entries from leader=%d, size=%d\n", args.LeaderID, rf.logCount)
	}

	// update commit index (after prev log entry is valid)
	if args.LeaderCommit > rf.commitIndex {
		lastNewLogIndex := args.PrevLogIndex + len(args.Entries)
		if lastNewLogIndex < args.LeaderCommit {
			rf.commitIndex = lastNewLogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}

		/* go func() {
			rf.commitCh <- true
		}() */
		rf.checkCommit()
		debug(rf.me, rf.currentTerm, "update commit from leader=%d to %d\n", args.LeaderID, rf.commitIndex)
	}
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
// may fail or lose an election.
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
	isLeader = (rf.role == leader)
	term = rf.currentTerm
	if isLeader {
		entry := &logEntry{
			Term:    rf.currentTerm,
			Command: command,
		}
		if rf.logCount < len(rf.log) {
			rf.log[rf.logCount] = entry
		} else {
			rf.log = append(rf.log, entry)
		}
		rf.logCount++
		index = rf.logCount
		rf.persist()
		debug(rf.me, rf.currentTerm, "append command=%v to index=%d\n", command, index)
		drainAndAdd(rf.appendEntriesCh, true)
		// debug(rf.me, rf.currentTerm, "Start() succeeds, command=%v, index=%d\n", command, index)
	} else {
		// debug(rf.me, rf.currentTerm, "not a leader, skip Start, command=%v\n", command)
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	// stop election goroutine and append entries goroutine

	rf.debug("Kill()\n")
	// FIXME consider other solutions (maybe use condition variable)
	// send until block
	rf.mu.Lock()
	rf.role = follower
	rf.c.Broadcast()
	rf.mu.Unlock()
	for {
		select {
		case rf.quit <- true:
		default:
			// FIXME maybe use waitgroup to wait for all of the goroutines finished
			return
		}
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
	rf.c = sync.NewCond(&rf.mu)

	rf.commitCh = make(chan bool)
	rf.electionTimerCh = make(chan bool)
	rf.appendEntriesCh = make(chan bool, 1)
	rf.replicationCh = make(chan bool)
	rf.quit = make(chan bool)
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// start as follower
	rf.role = follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.log == nil {
		rf.log = make([]*logEntry, 0)
	}

	// election goroutine
	go startElectionWorker(rf)

	// append entries goroutine
	go startAppendEntriesWorkers(rf)

	// check replication status and update commitIndex goroutine
	// go startReplicationStatusChecker(rf)

	// apply committed log entries
	go startLogEntryWorker(rf, applyCh)

	return rf
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == leader {
		debug(rf.me, rf.currentTerm, "already leader, skipping election timeout, term=%d\n", rf.currentTerm)
		return
	}
	debug(rf.me, rf.currentTerm, "starts election, new term=%d\n", rf.currentTerm+1)

	// increase term
	rf.currentTerm++
	// change to candidate role
	rf.role = candidate
	// vote itself
	rf.votedFor = &rf.me
	rf.persist()
	eleTerm := rf.currentTerm
	// channel for granted votes
	voteCh := make(chan int)
	// # of votes required to become a leader
	votesRequired := (len(rf.peers) + 1) / 2
	wg := &sync.WaitGroup{}
	wg.Add(len(rf.peers))

	// handle granted vote
	go rf.startVoteHandler(voteCh, votesRequired, eleTerm)

	for i := range rf.peers {
		if i == rf.me {
			// skip itself
			wg.Done()
			continue
		}

		// send vote request
		go rf.sendVoteRequest(i, eleTerm, wg, voteCh)
	}

	// wait for all vote request to finish
	go func() {
		wg.Wait()
		debug(rf.me, eleTerm, "all vote req goroutine finished\n")
		close(voteCh)
	}()
}

func (rf *Raft) checkTerm(term int) {
	rf.mu.Lock()
	if term > rf.currentTerm {
		debug(rf.me, rf.currentTerm, "receives higher term=%d, change to follower\n", term)
		// switch back to follower
		rf.currentTerm = term
		rf.votedFor = nil
		rf.role = follower
		rf.persist()
		rf.electionTimerCh <- true
		rf.c.Broadcast()
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendVoteRequest(server int, term int, wg *sync.WaitGroup, voteCh chan int) {
	var currTerm int
	var role peerRole
	for {
		lastLogIndex := 0
		lastLogTerm := 0
		rf.mu.Lock()
		currTerm = rf.currentTerm
		role = rf.role
		if rf.logCount > 0 {
			lastLogIndex = rf.logCount
			lastLogTerm = rf.log[rf.logCount-1].Term
		}
		rf.mu.Unlock()

		if currTerm != term || role != candidate {
			// stale request
			debug(rf.me, term, "already pass this term, not sending vote req, currTerm=%d\n", currTerm)
			return
		}

		debug(rf.me, term, "sends vote req to peer=%d, lastLogIndex=%d, lastLogTerm=%d\n",
			server, lastLogIndex, lastLogTerm)
		args := &RequestVoteArgs{
			Term:         currTerm,
			CandidateID:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply := &RequestVoteReply{}
		replyCh := make(chan bool, 1)
		go func() {
			replyCh <- rf.sendRequestVote(server, args, reply)
		}()

		success := false
		select {
		case success = <-replyCh:
		case <-time.After(rpcTimeout * time.Millisecond):
			debug(rf.me, currTerm, "vote request to peer=%d timeout\n", server)
		}

		if success {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				debug(rf.me, rf.currentTerm, "receives higher term=%d, change to follower\n", reply.Term)
				// switch back to follower
				rf.currentTerm = reply.Term
				rf.votedFor = nil
				rf.role = follower
				rf.persist()
				rf.mu.Unlock()
				return
			}
			if rf.currentTerm != currTerm {
				debug(rf.me, rf.currentTerm, "vote request sent in old term=%d, ignore\n", currTerm)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			if reply.VoteGranted {
				// report getting voted to leader
				voteCh <- server
			} else {
				debug(rf.me, currTerm, "vote req rejected by peer=%d\n", server)
			}
			break
		} else {
			debug(rf.me, term, "fail to send vote req to peer=%d, retrying\n", server)
		}
	}
	wg.Done()
}

func (rf *Raft) startVoteHandler(ch chan int, votesRequired int, term int) {
	voteRecords := make(map[int]bool)
	count := 1
	for server := range ch {
		if _, ok := voteRecords[server]; !ok {
			voteRecords[server] = true
			count++
			debug(rf.me, term,
				"vote granted by peer=%d, total=%d, required=%d\n", server, count, votesRequired)
		}
		if count == votesRequired {
			// elected as leader
			debug(rf.me, term, "got enough votes, count=%d\n", count)
			break
		}
	}
	rf.mu.Lock()
	if rf.currentTerm == term && rf.role == candidate && count >= votesRequired {
		// still in this term

		// change role
		rf.role = leader
		rf.persist()
		rf.initializeLeaderState()
		// TODO consider to stop timer entirely
		rf.electionTimerCh <- false
		debug(rf.me, term, "elected as the leader in term=%d\n", rf.currentTerm)
		// reset heartbeat timer only
		rf.c.Broadcast()
		drainAndAdd(rf.appendEntriesCh, true)
	} else {
		// not enough vote
		rf.role = follower
		rf.votedFor = nil
		rf.persist()
		debug(rf.me, term, "not elected, change to follower, currTerm=%d, role=%d, grantedVotes=%d\n",
			rf.currentTerm, rf.role, count)
	}
	rf.mu.Unlock()
}

func (rf *Raft) debug(format string, a ...interface{}) {
	rf.mu.Lock()
	serverStr := fmt.Sprintf("[s=%d,t=%d] ", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	DPrintf(serverStr+format, a...)
}

func (rf *Raft) checkCommit() {
	applyCh := rf.applyCh
	if rf.lastApplied < rf.commitIndex {
		// i = log slice index = log index - 1
		for i := rf.lastApplied; i < rf.commitIndex; i++ {
			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: i + 1,
				Command:      rf.log[i].Command,
			}
			applyCh <- msg
		}
		debug(rf.me, rf.currentTerm, "apply log entries from %d to %d\n", rf.lastApplied+1, rf.commitIndex)
		rf.lastApplied = rf.commitIndex
	}
}

func (rf *Raft) initializeLeaderState() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// initialize to last log index + 1
	// NOTE log index starts at 1
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.logCount + 1
	}
}

func (rf *Raft) checkReplicationStatus() {
	quorem := (len(rf.peers) + 1) / 2
	oldCommitIndex := rf.commitIndex
	for i := oldCommitIndex + 1; i <= rf.logCount; i++ {
		// check if this log entry has been replicated to the majority servers
		// and check if the commit is on this term
		count := 1
		for server := range rf.peers {
			if server == rf.me {
				// skip self
				continue
			}

			if rf.matchIndex[server] >= i {
				// server has log entry with index i
				count++
			}
		}

		debug(rf.me, rf.currentTerm, "checking log index=%d, replicas=%d\n", i, count)
		if count < quorem {
			// not reach quorem
			break
		} else if rf.log[i-1].Term != rf.currentTerm {
			// previous term, cannot commit
			debug(rf.me, rf.currentTerm,
				"log index=%d is in previous term=%d, not commit\n", i, rf.log[i-1].Term)
			continue
		} else {
			rf.commitIndex = i
		}
	}
	debug(rf.me, rf.currentTerm, "prev commit=%d, curr commit=%d\n", oldCommitIndex, rf.commitIndex)
	if rf.commitIndex > oldCommitIndex {
		// signal commit changes for worker to apply commited logs
		// rf.commitCh <- true
		rf.checkCommit()
	}
}

func debug(server int, term int, format string, a ...interface{}) {
	serverStr := fmt.Sprintf("[s=%d,t=%d] ", server, term)
	DPrintf(serverStr+format, a...)
}

// check if logs have been replicated to majority of servers
func startReplicationStatusChecker(rf *Raft) {
	for {
		select {
		case <-rf.replicationCh:
			rf.mu.Lock()
			quorem := (len(rf.peers) + 1) / 2
			oldCommitIndex := rf.commitIndex
			for i := oldCommitIndex + 1; i <= rf.logCount; i++ {
				// check if this log entry has been replicated to the majority servers
				// and check if the commit is on this term
				count := 1
				for server := range rf.peers {
					if server == rf.me {
						// skip self
						continue
					}

					if rf.matchIndex[server] >= i {
						// server has log entry with index i
						count++
					}
				}

				debug(rf.me, rf.currentTerm, "checking log index=%d, replicas=%d\n", i, count)
				if count < quorem {
					// not reach quorem
					break
				} else if rf.log[i-1].Term != rf.currentTerm {
					// previous term, cannot commit
					debug(rf.me, rf.currentTerm,
						"log index=%d is in previous term=%d, not commit\n", i, rf.log[i-1].Term)
					continue
				} else {
					rf.commitIndex = i
				}
			}
			debug(rf.me, rf.currentTerm, "prev commit=%d, curr commit=%d\n", oldCommitIndex, rf.commitIndex)
			if rf.commitIndex > oldCommitIndex {
				// signal commit changes for worker to apply commited logs
				// rf.commitCh <- true
				rf.checkCommit()
			}
			rf.mu.Unlock()
		case <-rf.quit:
			rf.debug("stops replication status checker goroutine\n")
			return
		}
	}
}

// apply commit when commitIndex change
func startLogEntryWorker(rf *Raft, applyCh chan ApplyMsg) {
	// send ApplyMsg to applyCh
	for {
		select {
		case <-rf.commitCh:
			rf.mu.Lock()
			debug(rf.me, rf.currentTerm, "lastApplied=%d, commitIndex=%d\n", rf.lastApplied, rf.commitIndex)
			if rf.lastApplied < rf.commitIndex {
				// i = log slice index = log index - 1
				for i := rf.lastApplied; i < rf.commitIndex; i++ {
					msg := ApplyMsg{
						CommandValid: true,
						CommandIndex: i + 1,
						Command:      rf.log[i].Command,
					}
					applyCh <- msg
				}
				debug(rf.me, rf.currentTerm, "apply log entries from %d to %d\n", rf.lastApplied+1, rf.commitIndex)
				rf.lastApplied = rf.commitIndex
			}
			rf.mu.Unlock()
		case <-rf.quit:
			rf.debug("stops log entry goroutine\n")
			return
		}
	}
}

// when election timeout ellapse, start new election
func startElectionWorker(rf *Raft) {
	timer := resetTimer(rf, getElectionTimeoutDuration, electionTimerName)

	for {
		select {
		case <-timer.C:
			// timeout start election
			rf.startElection()
		case shouldReset := <-rf.electionTimerCh:
			// reset timer
			if shouldReset {
				timer = resetTimer(rf, getElectionTimeoutDuration, electionTimerName)
			} else {
				timer.Stop()
			}
		case <-rf.quit:
			rf.debug("stops election goroutine\n")
			return
		}
	}
}

// for AppendEntries (heartbeat and log)
func startAppendEntriesWorkers(rf *Raft) {
	/* triggers := make([]chan bool, len(rf.peers))
	for i := range rf.peers {
		triggers[i] = make(chan bool)
		if i != rf.me {
			go startAppendEntryWorker(rf, i, triggers[i])
		}
	} */

	for {
		rf.mu.Lock()
		for rf.role != leader {
			debug(rf.me, rf.currentTerm, "not a leader, waiting\n")
			rf.c.Wait()
			select {
			case <-rf.quit:
				rf.debug("stops append entries goroutine\n")
				return
			default:
			}
		}
		rf.mu.Unlock()
		select {
		case shouldSend := <-rf.appendEntriesCh:
			if shouldSend {
				startAppendEntryLoop(rf)
			}
		case <-time.After(time.Duration(getHeartbeatInterval()) * time.Millisecond):
			startAppendEntryLoop(rf)
		}
		time.Sleep(time.Duration(getHeartbeatInterval()) * time.Millisecond)
	}
}

/* func triggerAll(triggers []chan bool, shouldSend bool) {
	for _, ch := range triggers {
		ch := ch
		select {
		case ch <- shouldSend:
		default:
		}
	}
} */

func startAppendEntryLoop(rf *Raft) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			success := false
			rf.mu.Lock()
			term := rf.currentTerm
			rf.mu.Unlock()
			for !success {
				success = sendAppendEntriesRequest(rf, server, term)
			}
		}(i)
	}
}

/* func startAppendEntryWorker(rf *Raft, server int, ch chan bool) {
	timer := resetTimer(rf, getHeartbeatInterval, heartBeatTimerName)
	for {
		select {
		case <-timer.C:
			// timeout, send AppendEntries request
			timer.Stop()
			sendAppendEntriesRequest(rf, server)
			timer = resetTimer(rf, getHeartbeatInterval, heartBeatTimerName)
		case shouldSend := <-ch:
			// reset timer and send AppendEntries request
			timer = resetTimer(rf, getHeartbeatInterval, heartBeatTimerName)
			if shouldSend {
				timer.Stop()
				success := false
				for !success {
					success = sendAppendEntriesRequest(rf, server)
				}
				timer = resetTimer(rf, getHeartbeatInterval, heartBeatTimerName)
			}
		case <-rf.quit:
			return
		}
	}
} */

// send append entries requests to all servers
func sendAppendEntriesRequest(rf *Raft, server int, term int) bool {
	var currTerm int
	var role peerRole
	var entries []*logEntry
	// used to update nextIndex if the request succeeds
	var nextLogIndex int
	prevLogIndex := 0
	prevLogTerm := 0
	rf.mu.Lock()
	currTerm = rf.currentTerm
	role = rf.role
	if role == leader {
		// generate unsent log entries
		nextIndex := rf.nextIndex[server]
		if rf.logCount != 0 && nextIndex <= rf.logCount {
			entries = rf.log[nextIndex-1 : rf.logCount]
			debug(rf.me, currTerm, "sending entries[%d:%d] to peer=%d\n", nextIndex-1, rf.logCount, server)
		}
		nextLogIndex = rf.logCount + 1
		// set previous log entry (if nextIndex is not 1)
		if nextIndex > 1 {
			prevLogIndex = nextIndex - 1
			prevLogTerm = rf.log[nextIndex-2].Term
			debug(rf.me, currTerm, "peer=%d, prevLogIndex=%d, prevLogTerm=%d\n", server, prevLogIndex, prevLogTerm)
		}
	}
	rf.mu.Unlock()

	if role != leader || currTerm != term {
		// not leader anymore, or term changed
		// debug(rf.me, currTerm, "not a leader, not sending AppendEntries requests\n")
		return true
	}
	debug(rf.me, currTerm, "sending heartbeat to server=%d\n", server)
	args := &AppendEntriesArgs{
		Term:         currTerm,
		LeaderID:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
	}
	reply := &AppendEntriesReply{}

	replyCh := make(chan bool, 1)
	go func() {
		replyCh <- rf.sendAppendEntries(server, args, reply)
	}()

	success := false
	select {
	case success = <-replyCh:
	case <-time.After(rpcTimeout * time.Millisecond):
		debug(rf.me, currTerm, "AppendEntries to peer=%d timeout\n", server)
	}

	if !success {
		debug(rf.me, currTerm, "fail to send AppendEntries to peer=%d\n", server)
	} else {
		rf.mu.Lock()
		// check if the peer has higher term (if yes, become a follower)
		if reply.Term > rf.currentTerm {
			debug(rf.me, rf.currentTerm, "receives higher term=%d, change to follower\n", reply.Term)
			// switch back to follower
			rf.currentTerm = reply.Term
			rf.votedFor = nil
			rf.role = follower
			rf.persist()
			rf.electionTimerCh <- true
			rf.mu.Unlock()
			return true
		}
		if rf.role != leader {
			debug(rf.me, rf.currentTerm, "ignore append entries response, not leader anymore\n")
			rf.mu.Unlock()
			return true
		}
		if rf.currentTerm != currTerm {
			// not in the same term as we send this request, ignore this response
			debug(rf.me, rf.currentTerm, "AppendEntries sent in old term=%d, ignore\n", currTerm)
			rf.mu.Unlock()
			return true
		}
		if !reply.Success {
			debug(rf.me, currTerm, "AppendEntries rejected by peer=%d\n", server)
			// index not changed
			rf.nextIndex[server] = max(1, reply.FirstLogIndex)
			/* if reply.ConflictTerm == 0 {
				rf.nextIndex[server] = max(1, reply.FirstLogIndex)
			} else {
				lastTermIndex := rf.logCount
				for ; lastTermIndex >= 1; lastTermIndex-- {
					if rf.log[lastTermIndex-1].Term == reply.ConflictTerm {
						break
					} else if rf.log[lastTermIndex-1].Term < reply.ConflictTerm {
						// already pass the term
						lastTermIndex = -1
						break
					}
				}

				if lastTermIndex == -1 {
					rf.nextIndex[server] = max(reply.FirstLogIndex, 1)
				} else {
					rf.nextIndex[server] = lastTermIndex + 1
				}
			} */
			debug(rf.me, currTerm, "decrement nextIndex[%d] to %d\n", server, rf.nextIndex[server])
		} else {
			// update matchIndex and nextIndex
			if nextLogIndex > rf.nextIndex[server] {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				// check if commit index can advance
				rf.checkReplicationStatus()
				debug(rf.me, currTerm, "update peer=%d, nextIndex to %d, matchIndex to %d\n",
					server, rf.nextIndex[server], rf.matchIndex[server])
			}
		}
		rf.mu.Unlock()
	}
	return !success || reply.Success
}

func resetTimer(rf *Raft, getDuration func() int, timerName string) *time.Ticker {
	duration := getDuration()
	timer := time.NewTicker(time.Duration(duration) * time.Millisecond)
	// rf.debug("resets %s timer, duration=%d\n", timerName, duration)
	return timer
}

func getHeartbeatInterval() int {
	return heartBeatInterval
}

func getElectionTimeoutDuration() int {
	return rand.Intn(maxElectionTimeout-minElectionTimeout+1) + minElectionTimeout
}

func max(v1 int, v2 int) int {
	if v1 > v2 {
		return v1
	}
	return v2
}

func drainAndAdd(c chan bool, val bool) {
	select {
	case <-c:
	default:
	}
	c <- val
}
