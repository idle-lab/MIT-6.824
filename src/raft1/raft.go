package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"math/bits"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)
const (
	heartbeatInterval     = 50 * time.Millisecond
	electionTimeoutBegin  = 400 // ms
	electionTimeoutLength = 200
)

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
		ticker: time.NewTicker(time.Hour),
	}
}

func (r *randomTicker) Reset() {
	d := r.begin + int(rand.Int31())%r.length
	r.ticker.Reset(time.Duration(d) * time.Millisecond)
}

func (r *randomTicker) C() <-chan time.Time {
	return r.ticker.C
}

func (r *randomTicker) Stop() {
	r.ticker.Stop()
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan raftapi.ApplyMsg

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
	repCount    []int

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	rf.persister.Save(w.Bytes(), nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm int
		votedFor    int
		log         []RaftLog
	)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("decode failed.")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.repCount = make([]int, len(rf.log))
	DPrintf("[server %d] restart\n", rf.me)
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

func (rf *Raft) updateTerm(term int) {
	if rf.currentTerm == term {
		return
	}
	rf.currentTerm = term
	rf.persist()
}

func (rf *Raft) updateVoteFor(id int) {
	if rf.votedFor == id {
		return
	}
	rf.votedFor = id
	rf.persist()
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
		DPrintf("[%s %d %d] reject vote for %d, because its term is %d which is out of date\n", rf.stateName, rf.me, rf.currentTerm, args.CandidateId, args.Term)
		rf.mu.Unlock()
		return
	}

	if rf.currentTerm < args.Term {
		if rf.state != FOLLOWER {
			DPrintf("[%s %d %d] become follower, because there is a candidate with lager term %d\n", rf.stateName, rf.me, rf.currentTerm, args.Term)
			rf.becomeFollower(args.Term)
		}
		rf.updateVoteFor(-1)
	}

	rf.updateTerm(args.Term)
	reply.Term = args.Term

	lastIdx := len(rf.log) - 1
	if lastIdx > 0 && (rf.log[lastIdx].Term > args.LastLogTerm ||
		(rf.log[lastIdx].Term == args.LastLogTerm && rf.log[lastIdx].Index > args.LastLogIndex)) {
		DPrintf("[%s %d %d] reject vote for %d, because its log is not up to date\n", rf.stateName, rf.me, rf.currentTerm, args.CandidateId)
		rf.mu.Unlock()
		return
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("[%s %d %d] reject vote for %d, because it has voted for %d\n", rf.stateName, rf.me, rf.currentTerm, args.CandidateId, rf.votedFor)
		rf.mu.Unlock()
		return
	}

	reply.VoteGranted = true
	rf.updateVoteFor(args.CandidateId)
	DPrintf("[%s %d %d] voted for %d, term %d\n", rf.stateName, rf.me, rf.currentTerm, args.CandidateId, args.Term)
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
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Success = false

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if args.Term >= rf.currentTerm {
		if rf.state != FOLLOWER {
			rf.becomeFollower(args.Term)
			DPrintf("[%s %d %d] become follower, because there is a leader with lager term %d\n", rf.stateName, rf.me, rf.currentTerm, args.Term)
		}
		rf.updateVoteFor(args.LeaderId)
	}

	rf.updateTerm(args.Term)
	reply.Term = args.Term

	if args.Entries == nil {
		DPrintf("[%s %d %d] received heartbeat from %d, term %d\n", rf.stateName, rf.me, rf.currentTerm, args.LeaderId, args.Term)
	} else {
		DPrintf("[%s %d %d] received append entries from %d, term %d, prevLogIndex %d, prevLogTerm %d\n", rf.stateName, rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm)
	}

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%s %d %d] reject append entries from %d, because prevLogIndex %d is out of range or prevLogTerm %d is not match\n", rf.stateName, rf.me, rf.currentTerm, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
		reply.XIndex = -1
		reply.XLen = -1
		reply.XTerm = -1
		if args.PrevLogIndex >= len(rf.log) {
			reply.XLen = len(rf.log)
		} else {
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			for i := args.PrevLogIndex; i > 0 && i > rf.commitIndex; i-- {
				if rf.log[i].Term != reply.XTerm {
					break
				}
				reply.XIndex = rf.log[i].Index
			}
		}
		rf.mu.Unlock()

		rf.heartbeatCh <- struct{}{}
		return
	}

	for i := range args.Entries {
		if args.Entries[i].Index < len(rf.log) {
			rf.log = rf.log[:args.Entries[i].Index]
		}
		if args.Entries[i].Index != len(rf.log) {
			panic("args.Entries[i].Index != len(rf.log)")
		}
		rf.log = append(rf.log, args.Entries[i])
	}

	DPrintf("[%s %d %d] cur log [%v]\n", rf.stateName, rf.me, rf.currentTerm, rf.log)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	// apply the log to the state machine
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Cmd,
			CommandIndex: rf.log[i].Index,
		}
		DPrintf("[%s %d %d] apply log %v, index %d, term %d\n", rf.stateName, rf.me, rf.currentTerm, rf.log[i].Cmd, rf.log[i].Index, rf.log[i].Term)
	}
	rf.lastApplied = rf.commitIndex
	rf.repCount = append(rf.repCount, make([]int, len(args.Entries))...)
	reply.Success = true
	rf.persist()

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
	// DPrintf("[%s %d %d] send request vote to %d, term %d\n", rf.stateName, rf.me, rf.currentTerm,  server, args.Term)
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
	if args.Entries == nil {
		DPrintf("[%s %d %d] send heartbeat to %d, term %d\n", rf.stateName, rf.me, rf.currentTerm, server, args.Term)
	} else {
		DPrintf("[%s %d %d] send append entries to %d, term %d, prevLogIndex %d, prevLogTerm %d\n", rf.stateName, rf.me, rf.currentTerm, server, args.Term, args.PrevLogIndex, args.PrevLogTerm)
	}
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		isLeader = false
		return index, term, isLeader
	}

	log := RaftLog{
		Cmd:   command,
		Term:  rf.currentTerm,
		Index: rf.nextIndex[rf.me],
	}
	rf.log = append(rf.log, log)
	rf.repCount = append(rf.repCount, (1 << rf.me))
	rf.nextIndex[rf.me]++

	index = rf.log[len(rf.log)-1].Index
	term = rf.log[len(rf.log)-1].Term
	DPrintf("[leader %d %d] start command %v, index %d, term %d\n", rf.me, rf.currentTerm, command, index, term)
	DPrintf("[leader %d %d] cur log %v\n", rf.me, rf.currentTerm, rf.log)
	rf.persist()
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
	DPrintf("[%s %d] %s => follower \n", rf.stateName, rf.me, rf.stateName)
	rf.currentTerm = term
	// reset the votedFor to -1
	rf.votedFor = -1
	rf.stateName = "follower"
	rf.state = FOLLOWER

	rf.persist()
}

func (rf *Raft) becomeCandidate() {
	if rf.state == LEADER {
		panic("leader become candidate")
	}
	if rf.state == FOLLOWER {
		DPrintf("[follower %d %d] follower => candidate\n", rf.me, rf.currentTerm+1)
	} else {
		DPrintf("[candidate %d %d] election timeout\n", rf.me, rf.currentTerm+1)
	}
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.stateName = "candidate"
	rf.state = CANDIDATE

	rf.persist()
}

func (rf *Raft) becomeLeader() {
	if rf.state == FOLLOWER {
		panic("follower become leader")
	} else if rf.state == LEADER {
		panic("leader become leader")
	}
	rf.state = LEADER
	rf.stateName = "leader"
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	rf.persist()
	DPrintf("[candidate %d %d] candidate => leader\n", rf.me, rf.currentTerm)
	// log.SetPrefix(fmt.Sprintf("leader %d | ", rf.me) + log.Prefix())
}

// startHeartbeat sends heartbeat to all followers in parallel
func (rf *Raft) startHeartbeat() {
	// send heartbeat to all followers in parallel
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	var done bool
	var doneCh = make(chan struct{})

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			tick := time.NewTicker(heartbeatInterval)
			defer tick.Stop()
			for range tick.C {
				if rf.killed() {
					return
				}

				rf.mu.Lock()
				if done || term != rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[server] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				if rf.nextIndex[server] < len(rf.log) {
					args.Entries = rf.log[rf.nextIndex[server]:]
				}
				DPrintf("[%s %d %d] commitIndex %d, lastApplied %d, log %v\n", rf.stateName, rf.me, term, rf.commitIndex, rf.lastApplied, rf.log)
				rf.mu.Unlock()

				// send heartbeat should not be blocked
				go func() {
					var reply AppendEntriesReply
					if ok := rf.sendAppendEntries(server, &args, &reply); !ok {
						return
					}

					rf.mu.Lock()
					if done || term != rf.currentTerm {
						rf.mu.Unlock()
						return
					}

					if !reply.Success {
						if reply.Term > rf.currentTerm {
							rf.becomeFollower(reply.Term)
							done = true
							rf.mu.Unlock()
							doneCh <- struct{}{}
						} else {
							if reply.XLen != -1 {
								rf.nextIndex[server] = reply.XLen
							} else {
								rf.nextIndex[server] = reply.XIndex
							}
							DPrintf("[leader %d %d] send append entries to %d failed, update nextIndex to %d\n", rf.me, server, term, rf.nextIndex[server])
							rf.mu.Unlock()
						}
						return
					}

					if args.Entries == nil {
						DPrintf("[leader %d %d] send heartbeat to %d success\n", rf.me, server, term)
					} else {
						DPrintf("[leader %d %d] send append entries to %d success, range [%d, %d]\n", rf.me, server, term, args.Entries[0].Index, args.Entries[len(args.Entries)-1].Index)
					}

					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
					for i := range args.Entries {
						if args.Entries[i].Term != rf.currentTerm {
							continue
						}
						rf.repCount[args.Entries[i].Index] |= (1 << server)
						if bits.OnesCount32(uint32(rf.repCount[args.Entries[i].Index])) > len(rf.peers)/2 {
							rf.commitIndex = max(rf.commitIndex, args.Entries[i].Index)
						}
					}

					// apply the log to the state machine
					for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
						rf.applyCh <- raftapi.ApplyMsg{
							CommandValid: true,
							Command:      rf.log[i].Cmd,
							CommandIndex: rf.log[i].Index,
						}
					}
					rf.lastApplied = rf.commitIndex
					rf.mu.Unlock()
				}()
			}
		}(i)
	}
	// If receive heartbeat from another leader, current leader has become follower
	// and should stop sending heartbeat
	select {
	case <-rf.heartbeatCh:
		rf.mu.Lock()
		done = true
		rf.mu.Unlock()
	case <-doneCh:
	}
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
			if ok := rf.sendRequestVote(server, &args, &reply); !ok {
				return
			}
			DPrintf("[%s %d %d] received reply from %d, voteGranted %v\n", stateName, rf.me, args.Term, server, reply.VoteGranted)
			replyCh <- reply
		}(i)
	}

	DPrintf("[%s %d %d] start election\n", stateName, rf.me, args.Term)
	ticker := NewRandomTicker(electionTimeoutBegin, electionTimeoutLength)
	ticker.Reset()
	defer ticker.Stop()
	for !rf.killed() {
		select {
		case <-ticker.C():
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
	ticker := NewRandomTicker(electionTimeoutBegin, electionTimeoutLength)
	ticker.Reset()
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C():
			rf.mu.Lock()
			rf.becomeCandidate()
			rf.mu.Unlock()
			return
		case <-rf.heartbeatCh:
			rf.mu.Lock()
			DPrintf("[%s %d %d] commitIndex %d, lastApplied %d", rf.stateName, rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied)
			rf.mu.Unlock()
			ticker.Reset()
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
	labgob.Register(RaftLog{})

	// Your initialization code here (3A, 3B, 3C).
	rf.log = make([]RaftLog, 1)
	rf.repCount = make([]int, 1)
	rf.votedFor = -1
	rf.heartbeatCh = make(chan struct{}, 1)
	rf.state = FOLLOWER
	rf.stateName = "follower"
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
