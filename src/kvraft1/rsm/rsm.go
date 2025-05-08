package rsm

import (
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cmd any
	Me  int
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	lastAppliedIndex int
	nextCmdId        uint64
	resCh            map[int]*chan any
	resMu            map[int]*sync.Mutex
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			res := rsm.sm.DoOp(op.Cmd)
			log.Printf("[%d] apply op{type=%s, me=%d}, index %d\n", rsm.me, reflect.TypeOf(op.Cmd).Name(), op.Me, msg.CommandIndex)
			rsm.mu.Lock()
			if rsm.lastAppliedIndex+1 != msg.CommandIndex {
				panic(fmt.Sprintf("[%d] apply to rsm out of order, expect %d, got %d", rsm.me, rsm.lastAppliedIndex+1, msg.CommandIndex))
			}
			rsm.lastAppliedIndex++
			rsm.mu.Unlock()
			if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > rsm.maxraftstate {
				rsm.rf.Snapshot(msg.CommandIndex, rsm.sm.Snapshot())
			}
			if op.Me == rsm.me {
				rsm.mu.Lock()
				ch, ok := rsm.resCh[msg.CommandIndex]
				if !ok {
					rsm.mu.Unlock()
					continue
				}
				mu := rsm.resMu[msg.CommandIndex]
				log.Printf("[%d] return res{type=%s, me=%d}, index %d\n", rsm.me, reflect.TypeOf(op.Cmd).Name(), op.Me, msg.CommandIndex)
				rsm.mu.Unlock()
				mu.Lock()
				(*ch) <- res
				mu.Unlock()
			}

		} else if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)
			rsm.mu.Lock()
			rsm.lastAppliedIndex = max(rsm.lastAppliedIndex, msg.SnapshotIndex)
			rsm.mu.Unlock()
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:               me,
		maxraftstate:     maxraftstate,
		applyCh:          make(chan raftapi.ApplyMsg),
		sm:               sm,
		lastAppliedIndex: 0,
		nextCmdId:        1,
		resCh:            make(map[int]*chan any),
		resMu:            make(map[int]*sync.Mutex),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	ch := make(chan any)
	var mu sync.Mutex
	rsm.mu.Lock()
	op := Op{Cmd: req, Me: rsm.me}
	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	log.Printf("[%d] submit op{type=%s, me=%d}, index %d\n", rsm.me, reflect.TypeOf(op.Cmd).Name(), op.Me, index)
	rsm.resCh[index] = &ch
	rsm.resMu[index] = &mu
	rsm.mu.Unlock()
	defer func() {
		rsm.mu.Lock()
		delete(rsm.resCh, index)
		mu.Lock()
		close(ch)
		mu.Unlock()
		delete(rsm.resMu, index)
		rsm.mu.Unlock()
	}()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case res := <-ch:
			rsm.mu.Lock()
			log.Printf("[%d] return res%v, index %d\n", rsm.me, index, res)
			rsm.mu.Unlock()
			return rpc.OK, res
		case <-ticker.C:
			curTerm, isLeader := rsm.rf.GetState()
			rsm.mu.Lock()
			if rsm.lastAppliedIndex >= index || curTerm != term || !isLeader {
				log.Printf("[%d] submit op{type=%s, me=%d} failed\n", rsm.me, reflect.TypeOf(op.Cmd).Name(), op.Me)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			rsm.mu.Unlock()
		}
	}
}
