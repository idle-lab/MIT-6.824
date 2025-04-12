package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const FREE = "0"

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	l      string
	curVer rpc.Tversion
	uuid   string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	return &Lock{
		ck:     ck,
		l:      l,
		uuid:   kvtest.RandValue(8),
		curVer: 0,
	}
}

// Execute this function after Put operation to verify that the
// Put operation is executed at most once. The function
// returns true if the Put operation is executed at most once,
// otherwise returns false.
func (lk *Lock) checkAtMostOnce() bool {
	val, ver, err := lk.ck.Get(lk.l)
	if err == rpc.ErrNoKey {
		return false
	}
	lk.curVer = ver
	if val != lk.uuid {
		return false
	}
	return true
}

func (lk *Lock) putOnce(val string) bool {
	err := lk.ck.Put(lk.l, val, lk.curVer)
	switch err {
	case rpc.OK:
		lk.curVer++
		return true
	case rpc.ErrVersion:
		return false
	case rpc.ErrMaybe:
		return lk.checkAtMostOnce()
	default:
		panic("putOnce: unexpected error")
	}
}

func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.l)
		lk.curVer = ver
		if err == rpc.ErrNoKey {
			if lk.putOnce(lk.uuid) {
				return
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if val != FREE {
			// Lock is occupied by other clients
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if lk.putOnce(lk.uuid) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	err := lk.ck.Put(lk.l, FREE, lk.curVer)
	if err == rpc.ErrVersion {
		panic("Release: lock version mismatch")
	}
}
