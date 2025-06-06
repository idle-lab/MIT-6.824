package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/bits"
	"math/rand"
	"slices"
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
	heartbeatInterval     = 100 * time.Millisecond
	requestVoteInterval   = 200 * time.Millisecond
	electionTimeoutBegin  = 700 // ms
	electionTimeoutLength = 300
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCond *sync.Cond
	applyCh   chan raftapi.ApplyMsg
	snapshot  []byte
	startCh   chan struct{}

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
	return term, !rf.killed() && state == LEADER
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
	rf.persister.Save(w.Bytes(), rf.snapshot)
}

func (rf *Raft) sendHeartbateCh() {
	select {
	case rf.heartbeatCh <- struct{}{}:
	default:
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
	rf.lastApplied = max(0, log[0].Index-1)
	rf.commitIndex = log[0].Index
	rf.repCount = make([]int, len(rf.log))
	rf.snapshot = rf.persister.ReadSnapshot()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.log[0].Index {
		return
	}
	index = rf.toIndex(index)
	rf.log = rf.log[index:]
	rf.repCount = rf.repCount[index:]
	rf.snapshot = snapshot
	DPrintf("[%s %d %d] call Snapshot before %d, log[0]=%v\n", rf.stateName, rf.me, rf.currentTerm, index, rf.log[0])
	rf.persist()
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

func (rf *Raft) toIndex(index int) int {
	if index < rf.log[0].Index {
		panic(fmt.Sprintf("[%s %d %d] index=%d, log[0].Index=%d", rf.stateName, rf.me, rf.currentTerm, index, rf.log[0].Index))
	}
	if index-rf.log[0].Index >= len(rf.log) {
		DPrintf("[%s %d %d] index-rf.log[0].Index=%d, len(rf.log)=%d, nextIndex=%v\n", rf.stateName, rf.me, rf.currentTerm, index-rf.log[0].Index, len(rf.log), rf.nextIndex)
	}
	return index - rf.log[0].Index
}

func (rf *Raft) becomeFollower(term int) {
	if rf.state == FOLLOWER {
		panic("follower become follower")
	}
	DPrintf("[%s %d %d] %s => follower \n", rf.stateName, rf.me, rf.currentTerm, rf.stateName)
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
		DPrintf("[%s %d %d] follower => candidate\n", rf.stateName, rf.me, rf.currentTerm+1)
	} else {
		DPrintf("[%s %d %d] election timeout\n", rf.stateName, rf.me, rf.currentTerm+1)
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
	rf.repCount = make([]int, len(rf.log))

	for i := range rf.peers {
		rf.nextIndex[i] = rf.log[0].Index + len(rf.log)
		rf.matchIndex[i] = 0
	}

	rf.persist()
	DPrintf("[%s %d %d] candidate => leader\n", rf.stateName, rf.me, rf.currentTerm)
	// log.SetPrefix(fmt.Sprintf("leader %d | ", rf.me) + log.Prefix())
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
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
	if rf.log[lastIdx].Term > args.LastLogTerm ||
		(rf.log[lastIdx].Term == args.LastLogTerm && rf.log[lastIdx].Index > args.LastLogIndex) {
		DPrintf("[%s %d %d] reject vote for %d, because its log is not up to date, {%d,%d} > {%d,%d}\n", rf.stateName, rf.me, rf.currentTerm, args.CandidateId, rf.log[lastIdx].Term, rf.log[lastIdx].Index, args.LastLogTerm, args.LastLogIndex)
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

	rf.sendHeartbateCh()
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

	if args.PrevLogIndex < rf.log[0].Index {
		reply.Success = true
		rf.mu.Unlock()
		rf.sendHeartbateCh()
		return
	}

	idx := rf.toIndex(args.PrevLogIndex)
	if idx >= len(rf.log) || rf.log[idx].Term != args.PrevLogTerm {
		DPrintf("[%s %d %d] reject append entries from %d, because prevLogIndex %d is out of range or prevLogTerm %d is not match\n", rf.stateName, rf.me, rf.currentTerm, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
		reply.XIndex = -1
		reply.XLen = -1
		reply.XTerm = -1
		if idx >= len(rf.log) {
			reply.XLen = rf.log[0].Index + len(rf.log)
		} else {
			reply.XTerm = rf.log[idx].Term
			for i := idx; i > 0 && i > rf.commitIndex; i-- {
				if rf.log[i].Term != reply.XTerm {
					break
				}
				reply.XIndex = rf.log[i].Index
			}
		}
		rf.mu.Unlock()

		rf.sendHeartbateCh()
		return
	}

	for i := range args.Entries {
		j := rf.toIndex(args.Entries[i].Index)
		if j < len(rf.log) {
			if rf.log[j].Term != args.Entries[i].Term {
				rf.log = rf.log[:j]
			} else {
				continue
			}
		}
		if args.Entries[i].Index != rf.log[0].Index+len(rf.log) {
			panic(fmt.Sprintf("[%s %d] args.Entries[i].Index != rf.log[0].Index+len(rf.log)", rf.stateName, rf.me))
		}
		rf.log = append(rf.log, args.Entries[i])
	}

	DPrintf("[%s %d %d] cur log %v\n", rf.stateName, rf.me, rf.currentTerm, rf.log)

	rf.commitIndex = max(rf.commitIndex, min(args.LeaderCommit, rf.log[len(rf.log)-1].Index))
	rf.applyCond.Broadcast()
	rf.repCount = append(rf.repCount, make([]int, len(args.Entries))...)
	rf.persist()
	reply.Success = true

	rf.mu.Unlock()
	rf.sendHeartbateCh()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotArgs) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
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

	DPrintf("[%s %d %d] received install snapshot from %d, term %d, LastIncludedIndex %d, LastIncludedTerm %d\n", rf.stateName, rf.me, rf.currentTerm, args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)

	if args.LastIncludedIndex <= rf.commitIndex {
		rf.mu.Unlock()
		rf.sendHeartbateCh()
		return
	}

	rf.log = make([]RaftLog, 1)
	rf.log[0].Index = args.LastIncludedIndex
	rf.log[0].Term = args.LastIncludedTerm
	rf.lastApplied = args.LastIncludedIndex - 1
	rf.commitIndex = args.LastIncludedIndex
	rf.snapshot = args.Data
	rf.applyCond.Broadcast()
	rf.persist()

	rf.mu.Unlock()
	rf.sendHeartbateCh()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.state != LEADER {
		isLeader = false
		return index, term, isLeader
	}

	rf.log = append(rf.log, RaftLog{
		Cmd:   command,
		Term:  rf.currentTerm,
		Index: rf.nextIndex[rf.me],
	})
	rf.repCount = append(rf.repCount, (1 << rf.me))
	rf.nextIndex[rf.me]++

	index = rf.log[len(rf.log)-1].Index
	term = rf.log[len(rf.log)-1].Term
	DPrintf("[%s %d %d] start command %v, index %d, term %d\n", rf.stateName, rf.me, rf.currentTerm, command, index, term)
	DPrintf("[%s %d %d] cur log %v\n", rf.stateName, rf.me, rf.currentTerm, rf.log)
	rf.persist()
	select {
	case rf.startCh <- struct{}{}:
	default:
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
	rf.applyCond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// startHeartbeat sends heartbeat and replicated logs to all followers in parallel
func (rf *Raft) startHeartbeat() {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	done, doneCh := false, make(chan struct{})
	defer close(doneCh)
	tryLock := func() bool {
		rf.mu.Lock()
		if done {
			rf.mu.Unlock()
			return false
		}
		if term != rf.currentTerm {
			done = true
			rf.mu.Unlock()
			doneCh <- struct{}{}
			return false
		}
		return true
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			tick := time.NewTicker(heartbeatInterval)
			defer tick.Stop()
			for !rf.killed() {
				// When everything is over, we should not continue sending heartbeats, we should exit here.
				if !tryLock() {
					return
				}
				rf.mu.Unlock()

				// Send log entries should not be blocked
				go func() {
					rf.mu.Lock()
					if rf.state != LEADER {
						rf.mu.Unlock()
						return
					}
					if rf.nextIndex[server] <= rf.log[0].Index {
						args := InstallSnapshotArgs{
							Term:              term,
							LeaderId:          rf.me,
							LastIncludedIndex: rf.log[0].Index,
							LastIncludedTerm:  rf.log[0].Term,
							Data:              rf.snapshot,
						}
						DPrintf("[%s %d %d] send install snapshot to %d, snapshot=%v\n", rf.stateName, rf.me, term, server, rf.log[0])
						rf.mu.Unlock()

						var reply InstallSnapshotReply
						if ok := rf.sendInstallSnapshot(server, &args, &reply); !ok {
							return
						}

						if !tryLock() {
							return
						}
						DPrintf("[%s %d %d] send install snapshot to %d success, LastIncludedIndex=%d\n", rf.stateName, rf.me, term, server, args.LastIncludedIndex)
						if reply.Term > rf.currentTerm {
							rf.becomeFollower(reply.Term)
							done = true
							rf.mu.Unlock()
							doneCh <- struct{}{}
							return
						}

						rf.nextIndex[server] = args.LastIncludedIndex + 1
						rf.mu.Unlock()
					} else {
						// Followers haven't fallen too far behind, we send AppendEntries RPC.
						args := AppendEntriesArgs{
							Term:         term,
							LeaderId:     rf.me,
							PrevLogIndex: rf.nextIndex[server] - 1,
							PrevLogTerm:  rf.log[rf.toIndex(rf.nextIndex[server]-1)].Term,
							Entries:      nil,
							LeaderCommit: rf.commitIndex,
						}
						if rf.nextIndex[server] <= rf.log[len(rf.log)-1].Index {
							args.Entries = slices.Clone(rf.log[rf.toIndex(rf.nextIndex[server]):])
						}
						DPrintf("[%s %d %d] commitIndex %d, lastApplied %d, nextIndex=%v, log %v\n", rf.stateName, rf.me, term, rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.log)
						DPrintf("[%s %d %d] send append entries to %d, prevLogIndex %d, prevLogTerm %d, entries %v\n", rf.stateName, rf.me, term, server, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
						rf.mu.Unlock()

						var reply AppendEntriesReply
						if ok := rf.sendAppendEntries(server, &args, &reply); !ok {
							return
						}

						if !tryLock() {
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
								DPrintf("[leader %d %d] send append entries to %d failed, update nextIndex to %d\n", rf.me, term, server, rf.nextIndex[server])
								rf.mu.Unlock()
							}
							return
						}

						if args.Entries == nil {
							DPrintf("[%s %d %d] send heartbeat to %d success\n", rf.stateName, rf.me, term, server)
						} else if args.Entries[0].Index == rf.nextIndex[server] {
							DPrintf("[%s %d %d] send append entries to %d success, range [%d, %d]\n", rf.stateName, rf.me, term, server, args.Entries[0].Index, args.Entries[len(args.Entries)-1].Index)
							rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
							for i := range args.Entries {
								if args.Entries[i].Term != rf.currentTerm {
									continue
								}
								if rf.commitIndex < args.Entries[i].Index {
									j := rf.toIndex(args.Entries[i].Index)
									rf.repCount[j] |= (1 << server)
									if bits.OnesCount32(uint32(rf.repCount[j])) > len(rf.peers)/2 {
										rf.commitIndex = args.Entries[i].Index
									}
								}
							}
							rf.applyCond.Broadcast()
						}
						rf.mu.Unlock()
					}
				}()

				select {
				case <-tick.C:
				case <-rf.startCh:
				}
			}
			rf.mu.Lock()
			if !done {
				done = true
				rf.mu.Unlock()
				doneCh <- struct{}{}
				return
			}
			rf.mu.Unlock()
		}(i)
	}
	<-doneCh
}

// startElection starts a new election, and sends RequestVote RPC to all peers in parallel.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	stateName := rf.stateName
	rf.mu.Unlock()
	var voteCount int32 = 1
	done, doneCh := false, make(chan struct{})
	defer close(doneCh)
	tryLock := func() bool {
		rf.mu.Lock()
		if done {
			rf.mu.Unlock()
			return false
		}
		if rf.state != CANDIDATE || args.Term != rf.currentTerm {
			done = true
			rf.mu.Unlock()
			doneCh <- struct{}{}
			return false
		}
		return true
	}

	// send request vote in parallel
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			var reqDone atomic.Bool
			for !reqDone.Load() {
				if !tryLock() {
					return
				}
				rf.mu.Unlock()

				// send request vote should not be blocked
				go func() {
					var reply RequestVoteReply
					if ok := rf.sendRequestVote(server, &args, &reply); !ok {
						return
					}
					// Only one RequestVote should exectued.
					if !reqDone.CompareAndSwap(false, true) {
						return
					}

					if !tryLock() {
						return
					}
					DPrintf("[%s %d %d] received reply from %d, voteGranted %v\n", rf.stateName, rf.me, args.Term, server, reply.VoteGranted)
					if reply.VoteGranted {
						voteCount++
						if voteCount > int32(len(rf.peers))/2 {
							rf.becomeLeader()
							done = true
							rf.mu.Unlock()
							doneCh <- struct{}{}
							return
						}
					} else {
						if reply.Term > rf.currentTerm {
							rf.becomeFollower(reply.Term)
							done = true
							rf.mu.Unlock()
							doneCh <- struct{}{}
							return
						}
					}
					rf.mu.Unlock()
				}()

				time.Sleep(requestVoteInterval)
			}
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
			if done {
				<-doneCh
			}
			done = true
			if rf.state == CANDIDATE && args.Term == rf.currentTerm {
				rf.becomeCandidate()
			}
			rf.mu.Unlock()
			return
		case <-doneCh:
			return
		}
	}
}

// followerTick waits for heartbeat from leader, and if timeout, it becomes candidate.
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
			rf.startElection()
		case LEADER:
			rf.startHeartbeat()
		}
	}
}

func (rf *Raft) applyer() {
	defer close(rf.applyCh)
	for !rf.killed() {
		rf.mu.Lock()
		for !rf.killed() && rf.lastApplied == rf.commitIndex {
			rf.applyCond.Wait()
		}
		commitLogs := make([]raftapi.ApplyMsg, rf.commitIndex-rf.lastApplied)
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			j := rf.toIndex(i)
			if j == 0 {
				commitLogs = append(commitLogs, raftapi.ApplyMsg{
					CommandValid:  false,
					SnapshotValid: true,
					Snapshot:      rf.snapshot,
					SnapshotTerm:  rf.log[0].Term,
					SnapshotIndex: rf.log[0].Index,
				})
			} else {
				commitLogs = append(commitLogs, raftapi.ApplyMsg{
					CommandValid:  true,
					Command:       rf.log[j].Cmd,
					CommandIndex:  rf.log[j].Index,
					SnapshotValid: false,
				})
			}
		}
		DPrintf("[%s %d %d] apply logs=%v\n", rf.stateName, rf.me, rf.currentTerm, rf.log[rf.toIndex(rf.lastApplied+1):rf.toIndex(rf.commitIndex+1)])
		rf.mu.Unlock()
		for i := range commitLogs {
			if rf.killed() {
				return
			}
			rf.applyCh <- commitLogs[i]
		}
		rf.mu.Lock()
		if rf.lastApplied == lastApplied {
			rf.lastApplied = commitIndex
		}
		rf.mu.Unlock()
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
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.startCh = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.applyer()
	go rf.ticker()

	return rf
}
