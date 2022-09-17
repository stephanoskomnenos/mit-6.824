package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	ClientId  int64
	RequestId int
	mu        sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.ClientId = nrand()
	ck.RequestId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	// args := GetArgs{}

	// find leader
	// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)

	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := GetArgs{
		Key:       key,
		ClientId:  ck.ClientId,
		RequestId: ck.RequestId,
	}

	DPrintf("Client %d requests GET: %v", ck.ClientId, args)

	// valueCh := make(chan string)

	// for _, server := range ck.servers {
	// go func(server *labrpc.ClientEnd, valueCh chan string) {
	// 	for {
	// 		reply := GetReply{}
	// 		ok := server.Call("KVServer.Get", &args, &reply)
	// 		if !ok {
	// 			return
	// 		}

	// 		if reply.Err != ErrWrongLeader {
	// 			DPrintf("Client %d receives GET reply %v", ck.ClientId, reply)
	// 			valueCh <- reply.Value
	// 			break
	// 		}

	// 		time.Sleep(100 * time.Millisecond)
	// 	}
	// }(server, valueCh)
	// }
	// return <-valueCh

	for serverId := 0; ; serverId += 1 {
		reply := GetReply{}
		ok := ck.servers[serverId%len(ck.servers)].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			continue
		}

		if reply.Err != OK {
			DPrintf("Client %d request no.%d ERROR: %v", ck.ClientId, ck.RequestId, reply.Err)
		} else {
			DPrintf("Client %d receives GET key %v value %v", ck.ClientId, key, reply.Value)
		}
		ck.RequestId += 1
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.ClientId,
		RequestId: ck.RequestId,
	}

	DPrintf("Client %d requests PUT/APPEND on key %v args %v", ck.ClientId, key, args)

	// for _, server := range ck.servers {
	// 	go func(server *labrpc.ClientEnd) {
	// 		for {
	// 			reply := PutAppendReply{}
	// 			ok := server.Call("KVServer.PutAppend", &args, &reply)
	// 			if !ok {
	// 				return
	// 			}

	// 			if reply.Err == "" {
	// 				break
	// 			}

	// 			time.Sleep(100 * time.Millisecond)
	// 		}
	// 	}(server)
	// }

	for serverId := 0; ; serverId += 1 {
		reply := PutAppendReply{}
		ok := ck.servers[serverId%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			continue
		}

		if reply.Err != OK {
			DPrintf("Client %d PUT/APPEND request no.%d key %v value %v requestId %d ERROR: %v", ck.ClientId, ck.RequestId, key, value, ck.RequestId, reply.Err)
		} else {
			DPrintf("CLient %d receives PUT/APPEND request no.%d key %v value %v requestId %d", ck.ClientId, ck.RequestId, key, value, ck.RequestId)
		}
		ck.RequestId += 1
		break
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
