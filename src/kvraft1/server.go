package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type Value struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu sync.RWMutex
	mp map[string]Value
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch args := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}
		kv.mu.RLock()
		v, ok := kv.mp[args.Key]
		kv.mu.RUnlock()
		if !ok {
			reply.Err = rpc.ErrNoKey
			reply.Value = ""
			reply.Version = 0
		} else {
			reply.Err = rpc.OK
			reply.Value = v.Value
			reply.Version = v.Version
		}
		return reply
	case rpc.PutArgs:
		reply := rpc.PutReply{}
		kv.mu.Lock()
		if _, ok := kv.mp[args.Key]; ok {
			if kv.mp[args.Key].Version == args.Version {
				kv.mp[args.Key] = Value{Value: args.Value, Version: args.Version + 1}
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrVersion
			}
		} else {
			if args.Version == 0 {
				kv.mp[args.Key] = Value{Value: args.Value, Version: 1}
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrNoKey
			}
		}
		kv.mu.Unlock()
		return reply
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.mp)
	if err != nil {
		panic(err)
	}
	rsm.DPrintf("[%d] snapshot %v\n", kv.me, kv.mp)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	err := d.Decode(&kv.mp)
	if err != nil {
		panic(err)
	}
	rsm.DPrintf("[%d] restore %v\n", kv.me, kv.mp)
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	(*reply) = res.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	(*reply) = res.(rpc.PutReply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
	kv.rsm.Raft().Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	kv.mp = make(map[string]Value)
	return []tester.IService{kv, kv.rsm.Raft()}
}
