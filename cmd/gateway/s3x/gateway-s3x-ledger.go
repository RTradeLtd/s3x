package s3x

/* Design Notes
---------------

Internal functions should never claim or release locks.
Any claiming or releasing of locks should be done in the public setter+getter functions.
The reason for this is so that we can enable easy reuse of internal code.
*/

/////////////////////
// SETTER FUNCTINS //
/////////////////////

// AbortMultipartUpload is used to abort a multipart upload
func (le *ledgerStore) AbortMultipartUpload(bucketName, multipartID string) error {
	le.Lock()
	defer le.Unlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	if !ledger.bucketExists(bucketName) {
		return ErrLedgerBucketDoesNotExist
	}
	if err := ledger.multipartExists(multipartID); err != nil {
		return err
	}
	return ledger.deleteMultipartID(bucketName, multipartID)
}

// NewMultipartUpload is used to store the initial start of a multipart upload request
func (le *ledgerStore) NewMultipartUpload(bucketName, objectName, multipartID string) error {
	le.Lock()
	defer le.Unlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	if !ledger.bucketExists(bucketName) {
		return ErrLedgerBucketDoesNotExist
	}
	if ledger.MultipartUploads == nil {
		ledger.MultipartUploads = make(map[string]MultipartUpload)
	}
	ledger.MultipartUploads[multipartID] = MultipartUpload{
		Bucket: bucketName,
		Object: objectName,
		Id:     multipartID,
	}
	return nil //todo: save to ipfs
}

// PutObjectPart is used to record an individual object part within a multipart upload
func (le *ledgerStore) PutObjectPart(bucketName, objectName, partHash, multipartID string, partNumber int64) error {
	le.Lock()
	defer le.Unlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	if !ledger.bucketExists(bucketName) {
		return ErrLedgerBucketDoesNotExist
	}
	if err := ledger.multipartExists(multipartID); err != nil {
		return err
	}
	mpart := ledger.MultipartUploads[multipartID]
	mpart.ObjectParts = append(mpart.ObjectParts, ObjectPartInfo{
		Number:   partNumber,
		DataHash: partHash,
	})
	ledger.MultipartUploads[multipartID] = mpart
	return nil //todo: save to ipfs
}

// NewBucket creates a new ledger bucket entry
func (le *ledgerStore) NewBucket(name, hash string) error {
	le.Lock()
	defer le.Unlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	if ledger.bucketExists(name) {
		return ErrLedgerBucketExists
	}
	if ledger.GetBuckets() == nil {
		ledger.Buckets = make(map[string]LedgerBucketEntry)
	}
	ledger.Buckets[name] = LedgerBucketEntry{
		IpfsHash: hash,
	}
	return nil //todo: save to ipfs
}

// UpdateBucketHash is used to update the ledger bucket entry
// with a new IPFS hash
func (le *ledgerStore) UpdateBucketHash(name, hash string) error {
	le.Lock()
	defer le.Unlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	if !ledger.bucketExists(name) {
		return ErrLedgerBucketDoesNotExist
	}
	entry := ledger.Buckets[name]
	entry.IpfsHash = hash
	ledger.Buckets[name] = entry
	return nil //todo: save to ipfs
}

// RemoveObject is used to remove a ledger object entry from a ledger bucket entry
func (le *ledgerStore) RemoveObject(bucketName, objectName string) error {
	le.Lock()
	defer le.Unlock()
	return le.l.deleteObject(bucketName, objectName)
}

// AddObjectToBucket is used to update a ledger bucket entry with a new ledger object entry
func (le *ledgerStore) AddObjectToBucket(bucketName, objectName, objectHash string) error {
	le.Lock()
	defer le.Unlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	if !ledger.bucketExists(bucketName) {
		return ErrLedgerBucketDoesNotExist
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
	return nil //todo: save to ipfs
}

// DeleteBucket is used to remove a ledger bucket entry
func (le *ledgerStore) DeleteBucket(name string) error {
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
	return nil //todo: save to ipfs
}

// Close shuts down the ledger datastore
func (le *ledgerStore) Close() error {
	le.Lock()
	defer le.Unlock()
	return le.ds.Close()
}

/////////////////////
// GETTER FUNCTINS //
/////////////////////

// GetObjectParts is used to return multipart upload parts
func (le *ledgerStore) GetObjectParts(id string) ([]ObjectPartInfo, error) {
	le.RLock()
	defer le.RUnlock()
	ledger, err := le.getLedger()
	if err != nil {
		return nil, err
	}
	if err := ledger.multipartExists(id); err != nil {
		return nil, err
	}
	return ledger.GetMultipartUploads()[id].ObjectParts, nil
}

// MultipartIDExists is used to lookup if the given multipart id exists
func (le *ledgerStore) MultipartIDExists(id string) error {
	le.RLock()
	defer le.RUnlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	return ledger.multipartExists(id)
}

// IsEmptyBucket checks if a bucket is empty or not
func (le *ledgerStore) IsEmptyBucket(name string) error {
	le.RLock()
	defer le.RUnlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	if len(ledger.Buckets[name].Objects) == 0 {
		return nil
	}
	return ErrLedgerNonEmptyBucket
}

// BucketExists is a public function to check if a bucket exists
func (le *ledgerStore) BucketExists(name string) bool {
	le.RLock()
	defer le.RUnlock()
	ledger, err := le.getLedger()
	if err != nil {
		return false
	}
	return ledger.bucketExists(name)
}

// ObjectExists is a public function to check if an object exists, and returns the reason
// the object can't be found if any
func (le *ledgerStore) ObjectExists(bucketName, objectName string) error {
	le.RLock()
	defer le.RUnlock()
	ledger, err := le.getLedger()
	if err != nil {
		return err
	}
	return ledger.objectExists(bucketName, objectName)
}

// GetBucketHash is used to get the corresponding IPFS CID for a bucket
func (le *ledgerStore) GetBucketHash(name string) (string, error) {
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

// GetObjectHash is used to retrieve the corresponding IPFS CID for an object
func (le *ledgerStore) GetObjectHash(bucketName, objectName string) (string, error) {
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
func (le *ledgerStore) GetObjectHashes(bucket string) (map[string]string, error) {
	le.RLock()
	defer le.RUnlock()
	ledger, err := le.getLedger()
	if err != nil {
		return nil, err
	}
	if !ledger.bucketExists(bucket) {
		return nil, ErrLedgerBucketDoesNotExist
	}
	// maps object names to hashes
	var hashes = make(map[string]string, len(ledger.Buckets[bucket].Objects))
	for _, obj := range ledger.GetBuckets()[bucket].Objects {
		hashes[obj.GetName()] = obj.GetIpfsHash()
	}
	return hashes, err
}

// GetMultipartHashes returns the hashes of all multipart upload object parts
func (le *ledgerStore) GetMultipartHashes(bucket, multipartID string) ([]string, error) {
	le.RLock()
	defer le.RUnlock()
	ledger, err := le.getLedger()
	if err != nil {
		return nil, err
	}
	if !ledger.bucketExists(bucket) {
		return nil, ErrLedgerBucketDoesNotExist
	}
	if err := ledger.multipartExists(multipartID); err != nil {
		return nil, err
	}
	mpart := ledger.MultipartUploads[bucket]
	var hashes = make([]string, len(mpart.ObjectParts))
	for i, objpart := range mpart.ObjectParts {
		hashes[i] = objpart.GetDataHash()
	}
	return hashes, nil
}

// GetBucketNames is used to a slice of all bucket names our ledger currently tracks
func (le *ledgerStore) GetBucketNames() ([]string, error) {
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

// getLedger is used to return our Ledger object from storage, or return a cached version
func (le *ledgerStore) getLedger() (*Ledger, error) {
	if le.l == nil {
		ledger := &Ledger{}
		ledgerBytes, err := le.ds.Get(dsKey) //todo: change to per buck hash
		if err != nil {
			//todo: detect only key does not exist
			ledgerBytes, err := ledger.Marshal()
			if err != nil {
				panic(err)
			}
			if err := le.ds.Put(dsKey, ledgerBytes); err != nil {
				panic(err)
			}
		}
		if err := ledger.Unmarshal(ledgerBytes); err != nil {
			return nil, err
		}
		le.l = ledger
	}
	return le.l, nil
}

// multipartExists is a helper function to check if a multipart id exists in our ledger
// todo: document id
func (m *Ledger) multipartExists(id string) error {
	if m.MultipartUploads == nil {
		return ErrInvalidUploadID
	}
	if m.MultipartUploads[id].Id == "" {
		return ErrInvalidUploadID
	}
	return nil
}

// objectExists is a helper function to check if an object exists in our ledger.
func (m *Ledger) objectExists(bucket, object string) error {
	if m.GetBuckets()[bucket].Name == "" {
		return ErrLedgerBucketDoesNotExist
	}
	if m.GetBuckets()[bucket].Objects[object].Name == "" {
		return ErrLedgerObjectDoesNotExist
	}
	return nil
}

// bucketExists is a helper function to check if a bucket exists in our ledger
func (m *Ledger) bucketExists(name string) bool {
	return m.GetBuckets()[name].Name != ""
}

// putLedger is a helper function used to update the ledger store on disk
/*
func (le *LedgerStore) putLedger(ledger *Ledger) error {
	ledgerBytes, err := ledger.Marshal()
	if err != nil {
		return err
	}
	return le.ds.Put(dsKey, ledgerBytes)
}
*/

func (m *Ledger) deleteObject(bucketName, objectName string) error {
	bucket, ok := m.Buckets[bucketName]
	if !ok {
		return ErrLedgerBucketDoesNotExist
	}
	delete(bucket.Objects, objectName)
	//todo: save to ipfs
	return nil
}

func (m *Ledger) deleteBucket(bucketName string) error {
	bucket, ok := m.Buckets[bucketName]
	if !ok {
		return nil //already deleted or never existed
	}
	if len(bucket.Objects) != 0 {
		return ErrLedgerNonEmptyBucket
	}
	delete(m.Buckets, bucketName)
	//todo: save to ipfs
	return nil
}

func (m *Ledger) deleteMultipartID(bucketName, multipartID string) error {
	delete(m.MultipartUploads, multipartID)
	//todo: save to ipfs
	return nil
}
