package s3x

import (
	"sync"
)

type bucketLocker struct {
	m sync.Map
}

//read read locks on bucket and returns the unlock function,
//example: defer b.read(bucketName)()
func (b *bucketLocker) read(bucket string) func() {
	load, _ := b.m.LoadOrStore(bucket, &sync.RWMutex{})
	rw := load.(*sync.RWMutex)
	rw.RLock()
	return rw.RUnlock
}

//write write locks on bucket and returns the unlock function,
//example: defer b.write(bucketName)()
func (b *bucketLocker) write(bucket string) func() {
	load, _ := b.m.LoadOrStore(bucket, &sync.RWMutex{})
	rw := load.(*sync.RWMutex)
	rw.Lock()
	return rw.Unlock
}
