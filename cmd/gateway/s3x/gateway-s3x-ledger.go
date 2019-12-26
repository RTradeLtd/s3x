package s3x

import (
	"sync"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
)

/* Design Notes
---------------

Internal functions should never claim or release locks.
Any claiming or releasing of locks should be done in the public setter+getter functions.
The reason for this is so that we can enable easy reuse of internal code.
*/

var (
	ledgerKey    = datastore.NewKey("ledgerstatekey")
	ledgerPrefix = datastore.NewKey("ledgerRoot")
)

// LedgerStore is an internal bookkeeper that
// maps ipfs cids to bucket and object names
type LedgerStore struct {
	sync.RWMutex
	ds datastore.Batching
}

func newLedgerStore(ds datastore.Batching) *LedgerStore {
	ledger := &LedgerStore{
		ds: namespace.Wrap(ds, ledgerPrefix),
	}
	ledger.createLedgerIfNotExist()
	return ledger
}

/////////////////////
// SETTER FUNCTINS //
/////////////////////

// NewMultipartUpload is used to store the initial start of a multipart upload request
func (le *LedgerStore) NewMultipartUpload(bucketName, objectName, multipartID string) error {
	le.Lock()
	defer le.Unlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	if err := le.bucketExists(ledger, bucketName); err != nil {
		return err
	}
	if ledger.MultipartUploads == nil {
		ledger.MultipartUploads = make(map[string]MultipartUpload)
	}
	ledger.MultipartUploads[multipartID] = MultipartUpload{
		Bucket: bucketName,
		Object: objectName,
		Id:     multipartID,
	}
	return le.putLedger(ledger)
}

// PutObjectPart is used to record an individual object part within a multipart upload
func (le *LedgerStore) PutObjectPart(bucketName, objectName, partHash, multipartID string, partNumber int64) error {
	le.Lock()
	defer le.Unlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	if err := le.bucketExists(ledger, bucketName); err != nil {
		return err
	}
	if err := le.multipartExists(ledger, multipartID); err != nil {
		return err
	}
	mpart := ledger.MultipartUploads[multipartID]
	mpart.ObjectParts = append(mpart.ObjectParts, ObjectPartInfo{
		Number:   partNumber,
		DataHash: partHash,
	})
	ledger.MultipartUploads[multipartID] = mpart
	return le.putLedger(ledger)
}

// NewBucket creates a new ledger bucket entry
func (le *LedgerStore) NewBucket(name, hash string) error {
	le.Lock()
	defer le.Unlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	if err := le.bucketExists(ledger, name); err != nil {
		return err
	}
	if ledger.GetBuckets() == nil {
		ledger.Buckets = make(map[string]LedgerBucketEntry)
	}
	ledger.Buckets[name] = LedgerBucketEntry{
		Objects:  make(map[string]LedgerObjectEntry),
		Name:     name,
		IpfsHash: hash,
	}
	return le.putLedger(ledger)
}

// UpdateBucketHash is used to update the ledger bucket entry
// with a new IPFS hash
func (le *LedgerStore) UpdateBucketHash(name, hash string) error {
	le.Lock()
	defer le.Unlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	if err := le.bucketExists(ledger, name); err != nil {
		return err
	}
	entry := ledger.Buckets[name]
	entry.IpfsHash = hash
	ledger.Buckets[name] = entry
	return le.putLedger(ledger)
}

// RemoveObject is used to remove a ledger object entry from a ledger bucket entry
func (le *LedgerStore) RemoveObject(bucketName, objectName string) error {
	le.Lock()
	defer le.Unlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	if err := le.objectExists(ledger, bucketName, objectName); err != nil {
		return err
	}
	delete(ledger.Buckets[bucketName].Objects, objectName)
	return nil
}

// AddObjectToBucket is used to update a ledger bucket entry with a new ledger object entry
func (le *LedgerStore) AddObjectToBucket(bucketName, objectName, objectHash string) error {
	le.Lock()
	defer le.Unlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	if err := le.bucketExists(ledger, bucketName); err != nil {
		return err
	}
	// prevent nil map panic
	if ledger.GetBuckets()[bucketName].Objects == nil {
		bucket := ledger.Buckets[bucketName]
		bucket.Objects = make(map[string]LedgerObjectEntry)
		ledger.Buckets[bucketName] = bucket
	}
	ledger.Buckets[bucketName].Objects[objectName] = LedgerObjectEntry{
		Name:     objectName,
		IpfsHash: objectHash,
	}
	return le.putLedger(ledger)
}

// DeleteBucket is used to remove a ledger bucket entry
func (le *LedgerStore) DeleteBucket(name string) error {
	le.Lock()
	defer le.Unlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	if ledger.GetBuckets()[name].Name == "" {
		return ErrLedgerBucketDoesNotExist
	}
	delete(ledger.Buckets, name)
	return le.putLedger(ledger)
}

// Close shuts down the ledger datastore
func (le *LedgerStore) Close() error {
	le.Lock()
	defer le.Unlock()
	return le.ds.Close()
}

/////////////////////
// GETTER FUNCTINS //
/////////////////////

// MultipartIDExists is used to lookup if the given multipart id exists
func (le *LedgerStore) MultipartIDExists(id string) error {
	le.RLock()
	defer le.RUnlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	return le.multipartExists(ledger, id)
}

// BucketExists is a public function to check if a bucket exists
func (le *LedgerStore) BucketExists(name string) error {
	le.RLock()
	defer le.RUnlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	return le.bucketExists(ledger, name)
}

// ObjectExists is a public function to check if an object exists, and returns the reason
// the object can't be found if any
func (le *LedgerStore) ObjectExists(bucketName, objectName string) error {
	le.RLock()
	defer le.RUnlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	return le.objectExists(ledger, bucketName, objectName)
}

// GetBucketHash is used to get the corresponding IPFS CID for a bucket
func (le *LedgerStore) GetBucketHash(name string) (string, error) {
	le.RLock()
	defer le.RUnlock()
	ledger, err := le.getLedger()
	if err != nil {
		return "", err
	}
	if ledger.GetBuckets()[name].Name == "" {
		return "", ErrLedgerBucketDoesNotExist
	}
	return ledger.Buckets[name].IpfsHash, nil
}

// GetObjectHash is used to retrive the correspodning IPFS CID for an object
func (le *LedgerStore) GetObjectHash(bucketName, objectName string) (string, error) {
	le.RLock()
	defer le.RUnlock()
	ledger, err := le.getLedger()
	if err != nil {
		return "", err
	}
	if ledger.GetBuckets()[bucketName].Name == "" {
		return "", ErrLedgerBucketDoesNotExist
	}
	bucket := ledger.GetBuckets()[bucketName]
	if bucket.GetObjects()[objectName].Name == "" {
		return "", ErrLedgerObjectDoesNotExist
	}
	return bucket.GetObjects()[objectName].IpfsHash, nil
}

// GetObjectHashes gets a map of object names to object hashes for all objects in a bucket
func (le *LedgerStore) GetObjectHashes(bucket string) (map[string]string, error) {
	le.RLock()
	defer le.RUnlock()
	ledger, err := le.getLedger()
	if err != nil {
		return nil, err
	}
	if err := le.bucketExists(ledger, bucket); err != nil {
		return nil, err
	}
	// maps object names to hashes
	var hashes = make(map[string]string, len(ledger.Buckets[bucket].Objects))
	for _, obj := range ledger.GetBuckets()[bucket].Objects {
		hashes[obj.GetName()] = obj.GetIpfsHash()
	}
	return hashes, err
}

// GetBucketNames is used to a slice of all bucket names our ledger currently tracks
func (le *LedgerStore) GetBucketNames() ([]string, error) {
	le.RLock()
	defer le.RUnlock()
	ledger, err := le.getLedger()
	if err != nil {
		return nil, err
	}
	var (
		// maps bucket names to hashes
		names = make([]string, len(ledger.Buckets))
		count int
	)
	for _, b := range ledger.Buckets {
		names[count] = b.GetName()
		count++
	}
	return names, nil
}

///////////////////////
// INTERNAL FUNCTINS //
///////////////////////

// getLedger is used to return our Ledger object from storage
func (le *LedgerStore) getLedger() (*Ledger, error) {
	ledgerBytes, err := le.ds.Get(ledgerKey)
	if err != nil {
		return nil, err
	}
	ledger := &Ledger{}
	if err := ledger.Unmarshal(ledgerBytes); err != nil {
		return nil, err
	}
	return ledger, nil
}

// createLEdgerIfNotExist is a helper function to create our
// internal ledger store if it does not exist.
func (le *LedgerStore) createLedgerIfNotExist() {
	if _, err := le.getLedger(); err == nil {
		return
	}
	ledger := new(Ledger)
	ledgerBytes, err := ledger.Marshal()
	if err != nil {
		panic(err)
	}
	if err := le.ds.Put(ledgerKey, ledgerBytes); err != nil {
		panic(err)
	}
}

// multipartExists is a helper function to check if a multispart id exists in our ledger
func (le *LedgerStore) multipartExists(ledger *Ledger, id string) error {
	if ledger.MultipartUploads == nil {
		return ErrInvalidUploadID
	}
	if ledger.MultipartUploads[id].Id == "" {
		return ErrInvalidUploadID
	}
	return nil
}

// objectExists is a helper function to check if an object exists in our ledger.
func (le *LedgerStore) objectExists(ledger *Ledger, bucket, object string) error {
	if ledger.GetBuckets()[bucket].Name == "" {
		return ErrLedgerBucketDoesNotExist
	}
	if ledger.GetBuckets()[bucket].Objects[object].Name == "" {
		return ErrLedgerObjectDoesNotExist
	}
	return nil
}

// bucketExists is a helper function to check if a bucket exists in our ledger
func (le *LedgerStore) bucketExists(ledger *Ledger, name string) error {
	if ledger.GetBuckets()[name].Name != "" {
		return ErrLedgerBucketDoesNotExist
	}
	return nil
}

// putLedger is a helper function used to update the ledger store on disk
func (le *LedgerStore) putLedger(ledger *Ledger) error {
	ledgerBytes, err := ledger.Marshal()
	if err != nil {
		return err
	}
	return le.ds.Put(ledgerKey, ledgerBytes)
}
