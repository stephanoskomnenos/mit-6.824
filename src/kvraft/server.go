package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

const ExecutionTimeout = time.Millisecond * 10000

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Key   string
	Value string
	Type  string

	ClientId  int64
	RequestId int

	IsRepeated bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	storage       map[string]string
	waitChans     map[int](chan Op)
	lastRequestId map[int64]int
}

func (kv *KVServer) getWaitChan(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ch, ok := kv.waitChans[index]
	if !ok {
		ch = make(chan Op)
		kv.waitChans[index] = ch
	}
	return ch
}

func isSameRequest(p Op, q Op) bool {
	return p.ClientId == q.ClientId && p.RequestId == q.RequestId
}

func (kv *KVServer) isRepeatedRequest(clientId int64, requestId int) bool {
	lastReqId, exists := kv.lastRequestId[clientId]
	if !exists {
		return false
	}
	return lastReqId >= requestId
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{
		Type:       GET,
		Key:        args.Key,
		ClientId:   args.ClientId,
		RequestId:  args.RequestId,
		IsRepeated: false,
	}

	// reply NOT_LEADER if not leader
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		// DPrintf("Server %d is not leader", kv.me)
		return
	}

	DPrintf("Server %d receives GET request no.%d: %v", kv.me, index, op)

	waitCh := kv.getWaitChan(index)

	select {
	case appliedOp := <-waitCh:
		if !isSameRequest(op, appliedOp) {
			reply.Err = ErrTimeout
		} else {
			reply.Value = appliedOp.Value
			reply.Err = OK
			DPrintf("Server %d applied GET request no.%d", kv.me, index)
		}
	case <-time.After(ExecutionTimeout):
		reply.Err = ErrTimeout
		DPrintf("Server %d GET request no.%d timeout", kv.me, index)
	}

	close(kv.getWaitChan(index))
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Op{
		Type:       args.Op,
		Key:        args.Key,
		Value:      args.Value,
		ClientId:   args.ClientId,
		RequestId:  args.RequestId,
		IsRepeated: false,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		// DPrintf("Server %d is not leader", kv.me)
		return
	}

	DPrintf("Server %d receives PUT/APPEND request no.%d", kv.me, index)

	waitCh := kv.getWaitChan(index)

	select {
	case appliedOp := <-waitCh:
		if !isSameRequest(op, appliedOp) {
			reply.Err = ErrTimeout
		} else if appliedOp.IsRepeated {
			reply.Err = ErrRepeatedRequest
		} else {
			reply.Err = OK
			DPrintf("Server %d applied PUT/APPEND request no.%d, key %v, value %v", kv.me, index, appliedOp.Key, appliedOp.Value)
		}
	case <-time.After(ExecutionTimeout):
		reply.Err = ErrTimeout
		DPrintf("Server %d PUT/APPEND request no.%d timeout", kv.me, index)
	}

	close(kv.getWaitChan(index))
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:

			if applyMsg.CommandValid {
				// DPrintf("Server %d is applying request no.%d", kv.me, applyMsg.CommandIndex)
				kv.mu.Lock()
				op := applyMsg.Command.(Op)
				if kv.isRepeatedRequest(op.ClientId, op.RequestId) && op.Type != GET {
					kv.lastRequestId[op.ClientId] = op.RequestId
					op.IsRepeated = true
					DPrintf("Server %d receives repeated request no.%d from %d", kv.me, op.RequestId, op.ClientId)
				} else {
					switch op.Type {
					case GET:
						op.Value = kv.storage[op.Key]
					case PUT:
						kv.storage[op.Key] = op.Value
					case APPEND:
						kv.storage[op.Key] += op.Value
					}
				}
				kv.lastRequestId[op.ClientId] = op.RequestId
				kv.mu.Unlock()

				waitCh := kv.getWaitChan(applyMsg.CommandIndex)
				go func(op Op, ch chan Op) {
					ch <- op
				}(op, waitCh)
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.storage = make(map[string]string)
	kv.waitChans = make(map[int]chan Op)
	kv.lastRequestId = make(map[int64]int)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go kv.applier()

	return kv
}
