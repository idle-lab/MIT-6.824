package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)
const heartbeatInterval = 100 * time.Millisecond

type RaftLog struct {
	Cmd   interface{}
	Term  int
	Index int
}

type randomTicker struct {
	begin  int
	length int
	ticker *time.Ticker
}

func NewRandomTicker(begin, length int) *randomTicker {
	return &randomTicker{
		begin:  begin,
		length: length,
		ticker: time.NewTicker(1),
	}
}

func (r *randomTicker) Reset() {
	d := r.begin + int(rand.Int31())%r.length
	r.ticker.Reset(time.Duration(d) * time.Millisecond)
}

func (r *randomTicker) C() <-chan time.Time {
	return r.ticker.C
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	timer     *randomTicker

	// Leader state
	state       int // 0: follower, 1: candidate, 2: leader
	stateName   string
	heartbeatCh chan struct{}

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	// Updated on stable storage before responding to RPCs
	currentTerm int
	votedFor    int
	log         []RaftLog

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	// Reinitialized after election
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	state := rf.state
	rf.mu.Unlock()
	return term, state == LEADER
}

// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	reply.VoteGranted = false
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		log.Printf("%s %d reject vote from %d, because its term is %d which is out of date\n", rf.stateName, rf.me, args.CandidateId, args.Term)
		rf.mu.Unlock()
		return
	}

	if rf.currentTerm < args.Term {
		if rf.state != FOLLOWER {
			log.Printf("%s %d become follower, because there is a candidate with lager term %d\n", rf.stateName, rf.me, args.Term)
			rf.becomeFollower(args.Term)
		}
		rf.votedFor = -1
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term

	lastidx := len(rf.log) - 1
	if lastidx > 0 && (rf.log[lastidx].Term > args.LastLogTerm ||
		(rf.log[lastidx].Term == args.Term && rf.log[lastidx].Index > args.LastLogIndex)) {
		log.Printf("%s %d reject vote from %d, because its log is not up to date\n", rf.stateName, rf.me, args.CandidateId)
		rf.mu.Unlock()
		return
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		log.Printf("%s %d reject vote from %d, because it has voted for %d\n", rf.stateName, rf.me, args.CandidateId, rf.votedFor)
		rf.mu.Unlock()
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	log.Printf("%s %d voted for %d, term %d\n", rf.stateName, rf.me, args.CandidateId, args.Term)
	rf.mu.Unlock()
	rf.heartbeatCh <- struct{}{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []RaftLog
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Success = false

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		if rf.state != FOLLOWER {
			rf.becomeFollower(args.Term)
			log.Printf("%s %d become follower, because there is a leader with lager term %d\n", rf.stateName, rf.me, args.Term)
		}
		rf.votedFor = args.LeaderId
	}

	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	if args.Entries == nil {
		log.Printf("%s %d received heartbeat from %d, term %d\n", rf.stateName, rf.me, args.LeaderId, args.Term)
	}

	// if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	// 	return
	// }

	// for i := range args.Entries {
	// 	if args.Entries[i].Index < len(rf.log) {
	// 		rf.log = rf.log[:args.Entries[i].Index]
	// 	}
	// 	rf.log = append(rf.log, args.Entries[i])
	// }

	// if args.LeaderCommit > rf.commitIndex {
	// 	rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	// }

	reply.Success = true
	rf.mu.Unlock()

	rf.heartbeatCh <- struct{}{}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	return ok
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
	// Your code here (3B).
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

func (rf *Raft) becomeFollower(term int) {
	if rf.state == FOLLOWER {
		panic("follower become follower")
	}
	log.Printf("server %d become follower before %s\n", rf.me, rf.stateName)
	rf.currentTerm = term
	// reset the votedFor to -1
	rf.votedFor = -1
	rf.stateName = "follower"
	rf.state = FOLLOWER
	rf.timer.Reset()
}

func (rf *Raft) becomeCandidate() {
	if rf.state == LEADER {
		panic("leader become candidate")
	}
	if rf.state == FOLLOWER {
		log.Printf("server %d become candidate before follower\n", rf.me)
	} else {
		log.Printf("server %d become candidate because of timeout\n", rf.me)
	}
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.stateName = "candidate"
	rf.state = CANDIDATE
	rf.timer.Reset()
}

func (rf *Raft) becomeLeader() {
	if rf.state == FOLLOWER {
		panic("follower become leader")
	} else if rf.state == LEADER {
		panic("leader become leader")
	}
	rf.state = LEADER
	rf.stateName = "leader"

	log.Printf("candidate %d become leader\n", rf.me)
}

func (rf *Raft) sendHeartbeat(server int, args *AppendEntriesArgs) {
	stateName := "leader"
	var reply AppendEntriesReply
	if ok := rf.sendAppendEntries(server, args, &reply); ok {
		if reply.Success {
			log.Printf("%s %d send heartbeat to %d success\n", stateName, rf.me, server)
			// do something
		} else {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
			}
			rf.mu.Unlock()
		}
	} else {
		log.Printf("%s %d send heartbeat to %d failed\n", stateName, rf.me, server)
	}
}

func (rf *Raft) startHeartbeat() {
	// send heartbeat to all followers in parallel
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			for {
				if rf.killed() {
					return
				}

				rf.mu.Lock()
				if term != rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: len(rf.log) - 1,
					PrevLogTerm:  rf.log[len(rf.log)-1].Term,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()

				// send heartbeat should not be blocked
				go rf.sendHeartbeat(server, &args)
				time.Sleep(heartbeatInterval)
			}
		}(i)
	}

	// If receive heartbeat from another leader, current leader has become follower
	// and should stop sending heartbeat
	<-rf.heartbeatCh
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	stateName := rf.stateName
	rf.mu.Unlock()
	var voteCount int32 = 1

	replyCh := make(chan RequestVoteReply)
	// send request vote in parallel
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			var reply RequestVoteReply
			// send request vote should not be blocked
			if ok := rf.sendRequestVote(server, &args, &reply); ok {
				replyCh <- reply
			} else {
				log.Printf("%s %d send request vote to %d failed\n", stateName, rf.me, server)
			}
		}(i)
	}

	for !rf.killed() {
		rf.timer.Reset()
		select {
		case <-rf.timer.C():
			// restart a new election
			rf.mu.Lock()
			if args.Term != rf.currentTerm {
				rf.mu.Unlock()
				return
			}
			rf.becomeCandidate()
			rf.mu.Unlock()
			return
		case <-rf.heartbeatCh:
			// received heartbeat from leader
			return
		case reply := <-replyCh:
			rf.mu.Lock()
			if args.Term != rf.currentTerm {
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				voteCount++
				if voteCount > int32(len(rf.peers))/2 {
					rf.becomeLeader()
					rf.mu.Unlock()
					return
				}
			} else {
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					rf.mu.Unlock()
					return
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) followerTick() {
	rf.timer.Reset()
	for {
		select {
		case <-rf.heartbeatCh:
			rf.timer.Reset()
		case <-rf.timer.C():
			rf.mu.Lock()
			rf.becomeCandidate()
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case FOLLOWER:
			rf.followerTick()

		case CANDIDATE:
			// start a new election
			rf.startElection()

		case LEADER:
			// send heartbeat to all followers
			rf.startHeartbeat()
		}
	}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.log = make([]RaftLog, 1)
	rf.votedFor = -1
	rf.heartbeatCh = make(chan struct{}, 1)
	rf.state = FOLLOWER
	rf.stateName = "follower"
	rf.currentTerm = 0
	rf.timer = NewRandomTicker(700, 300)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
