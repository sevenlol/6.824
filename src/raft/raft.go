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
	"fmt"
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
	Term int
}

type peerRole int

const (
	follower peerRole = iota
	leader
	candidate
)

const (
	minElectionTimeout = 500
	maxElectionTimeout = 800
	heartBeatInterval  = 100
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
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

	/* volatile */
	role        peerRole
	commitIndex int
	lastApplied int

	/* leader */
	nextIndex  []int
	matchIndex []int

	/* other */
	electionTimerCh chan bool
	quit            chan bool
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int
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
	Term    int
	Success bool
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
		return
	}
	if rf.votedFor != nil && *rf.votedFor != args.CandidateID {
		return
	}

	reply.VoteGranted = true
	// request term is larger than currTerm
	rf.currentTerm = args.Term
	reply.Term = args.Term
	// vote for this request
	id := args.CandidateID
	rf.votedFor = &id
}

// AppendEntries heartbeat handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	leaderID := args.LeaderID

	// TODO check other arguments

	// accept leader request and change to follower
	rf.currentTerm = args.Term
	rf.votedFor = &leaderID
	rf.role = follower

	// configure reply
	reply.Term = args.Term
	reply.Success = true
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
	// stop election goroutine
	rf.quit <- true
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

	rf.electionTimerCh = make(chan bool)
	rf.quit = make(chan bool)

	// Your initialization code here (2A, 2B, 2C).

	// start as follower
	rf.role = follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.log == nil {
		rf.log = make([]*logEntry, 0)
	}

	// election goroutine
	go func() {
		timer := resetTimer(rf)

		for {
			select {
			case <-timer.C:
				// TODO timeout start election
				fmt.Printf("server=%d starts election, term=%d\n", me, rf.currentTerm)
				rf.startElection()
			case shouldReset := <-rf.electionTimerCh:
				// reset timer
				if shouldReset {
					DPrintf("server=%d resets election timer, term=%d\n", me, rf.currentTerm)
					timer = resetTimer(rf)
				} else {
					DPrintf("server=%d pauses election timer, term=%d\n", me, rf.currentTerm)
					timer.Stop()
				}
			case <-rf.quit:
				DPrintf("server=%d stops election goroutine\n", me)
				return
			}
		}
	}()

	return rf
}

func (rf *Raft) startElection() {
	// TODO implement election logic
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == leader {
		return
	}

	// increase term
	rf.currentTerm++
	// change to candidate role
	rf.role = candidate
	// vote itself
	rf.votedFor = &rf.me
	eleTerm := rf.currentTerm
	voteCh := make(chan int)
	// # of votes required to become a leader
	votesRequired := (len(rf.peers) + 1) / 2
	wg := &sync.WaitGroup{}
	wg.Add(len(rf.peers))

	// handle granted vote
	go func(ch chan int) {
		voteRecords := make(map[int]bool)
		count := 1
		for server := range ch {
			if _, ok := voteRecords[server]; !ok {
				voteRecords[server] = true
				count++
			}
			fmt.Printf("server=%d, vote=%d\n", rf.me, count)
			fmt.Println(votesRequired)
			if count == votesRequired {
				// elected as leader
				break
			}
		}
		rf.mu.Lock()
		if rf.currentTerm == eleTerm && rf.role == candidate {
			// still in this term
			rf.role = leader
			// TODO consider to stop timer entirely
			rf.electionTimerCh <- true
			DPrintf("server=%d becomes leader", rf.me)
			fmt.Printf("server=%d is the leader, term=%d\n", rf.me, rf.currentTerm)
			// sending heartbeat
			sendHeartBeat(rf, eleTerm)
		}
		rf.mu.Unlock()
	}(voteCh)

	for i := range rf.peers {
		if i == rf.me {
			// skip itself
			wg.Done()
			continue
		}

		// send vote request
		go func(server int, term int) {
			var currTerm int
			var role peerRole
			for {
				rf.mu.Lock()
				currTerm = rf.currentTerm
				role = rf.role
				rf.mu.Unlock()

				if currTerm != term || role != candidate {
					// stale request
					return
				}

				DPrintf("server=%d sends vote request to peer=%d, term=%d", rf.me, server, currTerm)

				reply := &RequestVoteReply{}
				success := rf.sendRequestVote(server, &RequestVoteArgs{currTerm, rf.me}, reply)
				if success {
					if reply.VoteGranted {
						// report getting voted to leader
						voteCh <- server
					}
					rf.checkTerm(reply.Term)
					break
				}
			}
			wg.Done()
		}(i, eleTerm)
	}

	// wait for all vote request to finish
	go func() {
		wg.Wait()
		// TODO add debug log
		close(voteCh)
	}()
}

func (rf *Raft) checkTerm(term int) {
	rf.mu.Lock()
	if term > rf.currentTerm {
		// switch back to follower
		rf.currentTerm = term
		rf.votedFor = nil
		rf.role = follower
	}
	rf.mu.Unlock()
}

func sendHeartBeat(rf *Raft, term int) {
	for i := range rf.peers {
		if i == rf.me {
			// skip itself
			continue
		}

		go func(server int) {
			for {
				var currTerm int
				var role peerRole
				rf.mu.Lock()
				currTerm = rf.currentTerm
				role = rf.role
				rf.mu.Unlock()

				if currTerm != term || role != leader {
					// not leader anymore or term changed
					return
				}
				args := &AppendEntriesArgs{
					Term:         term,
					LeaderID:     rf.me,
					LeaderCommit: rf.commitIndex,
					// TODO add other params
				}
				reply := &AppendEntriesReply{}
				success := rf.sendAppendEntries(server, args, reply)

				if !success {
					fmt.Printf("server=%d failed to send heartbeat to peer=%d\n", rf.me, server)
				} else {
					if !reply.Success {
						// TODO add log
					}
					rf.checkTerm(reply.Term)
				}

				time.Sleep(heartBeatInterval * time.Millisecond)
			}
		}(i)
	}
}

func resetTimer(rf *Raft) *time.Ticker {
	duration := getElectionTimeoutDuration()
	timer := time.NewTicker(time.Duration(duration) * time.Millisecond)
	DPrintf("peer=%d, election timeout=%d\n", rf.me, duration)
	return timer
}

func getElectionTimeoutDuration() int {
	return rand.Intn(maxElectionTimeout-minElectionTimeout+1) + minElectionTimeout
}
