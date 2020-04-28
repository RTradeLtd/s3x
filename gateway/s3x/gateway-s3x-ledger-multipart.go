package s3x

import (
	"github.com/ipfs/go-datastore"
	minio "github.com/minio/minio/cmd"
)

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
func (ls *ledgerStore) AbortMultipartUpload(bucket, multipartID string) error {
	err := ls.AssertBucketExits(bucket)
	if err != nil {
		return err
	}
	return ls.DeleteMultipartID(multipartID)
}

// NewMultipartUpload is used to store the initial start of a multipart upload request
func (ls *ledgerStore) NewMultipartUpload(multipartID string, info *ObjectInfo) error {
	bucket := info.GetBucket()
	err := ls.assertBucketExits(bucket)
	if err != nil {
		return err
	}
	m := &MultipartUpload{
		ObjectInfo:  info,
		Id:          multipartID,
		ObjectParts: make(map[int64]ObjectPartInfo),
	}
	ls.pmapLocker.Lock()
	ls.l.MultipartUploads[multipartID] = m
	ls.pmapLocker.Unlock()
	data, err := m.Marshal()
	if err != nil {
		return err
	}
	return ls.ds.Put(dsPartKey.ChildString(multipartID), data)
}

// PutObjectPart is used to record an individual object part within a multipart upload
func (ls *ledgerStore) PutObjectPart(bucketName, objectName, multipartID string, pi minio.PartInfo) error {
	pn := int64(pi.PartNumber)
	if pn > 10000 {
		return ErrInvalidPartNumber
	}

	err := ls.AssertBucketExits(bucketName)
	if err != nil {
		return err
	}

	defer ls.plocker.write(multipartID)()
	m, err := ls.getMultipartLoaded(multipartID)
	if err != nil {
		return err
	}
	if m.ObjectParts == nil {
		m.ObjectParts = make(map[int64]ObjectPartInfo)
	}
	m.ObjectParts[pn] = ObjectPartInfo{
		Number:       pn,
		Name:         objectName,
		LastModified: pi.LastModified,
		Size_:        pi.Size,
		ActualSize:   pi.ActualSize,
		DataHash:     pi.ETag,
	}
	data, err := m.Marshal()
	if err != nil {
		return err
	}
	return ls.ds.Put(dsPartKey.ChildString(multipartID), data)
}

/////////////////////
// GETTER FUNCTINS //
/////////////////////

// GetObjectParts is used to return multipart upload parts,
// returned unlock function must be used after map iteration is done.
func (ls *ledgerStore) GetObjectDetails(id string) (*MultipartUpload, func(), error) {
	unlock := ls.plocker.read(id)
	m, err := ls.getMultipartLoaded(id)
	if err != nil {
		unlock()
		return nil, nil, err
	}
	return m, unlock, nil
}

// MultipartIDExists is used to lookup if the given multipart id exists
func (ls *ledgerStore) MultipartIDExists(id string) error {
	return ls.assertValidUploadID(id)
}

// GetMultipartHashes returns the hashes of all multipart upload object parts
/* not used for now
func (ls *ledgerStore) GetMultipartHashes(bucket, multipartID string) ([]string, error) {
	ex, err := ls.bucketExists(bucket)
	if err != nil {
		return nil, err
	}
	if !ex {
		return nil, ErrLedgerBucketDoesNotExist
	}
	if err := ls.l.multipartExists(multipartID); err != nil {
		return nil, err
	}
	mpart := ls.l.MultipartUploads[bucket]
	var hashes = make([]string, len(mpart.ObjectParts))
	for i, objpart := range mpart.ObjectParts {
		hashes[i] = objpart.GetDataHash()
	}
	return hashes, nil
}*/

///////////////////////
// INTERNAL FUNCTINS //
///////////////////////

// assertValidUploadID is a helper function to check if a multipart id exists in our ledger
func (ls *ledgerStore) assertValidUploadID(uploadID string) error {
	_, err := ls.getMultipartLoaded(uploadID)
	return err
}

func (ls *ledgerStore) getMultipartLoaded(uploadID string) (*MultipartUpload, error) {
	m, err := ls.getMultipartNilable(uploadID)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, ErrInvalidUploadID
	}
	return m, nil
}

func (ls *ledgerStore) DeleteMultipartID(uploadID string) error {
	ls.pmapLocker.Lock()
	defer ls.pmapLocker.Unlock()
	delete(ls.l.MultipartUploads, uploadID)
	err := ls.ds.Delete(dsPartKey.ChildString(uploadID))
	if err == datastore.ErrNotFound {
		return ErrInvalidUploadID
	}
	return err
}

// getMultipartNilable returns a MultipartUpload or nil if it did not exist
func (ls *ledgerStore) getMultipartNilable(uploadID string) (*MultipartUpload, error) {
	ls.pmapLocker.Lock()
	defer ls.pmapLocker.Unlock()
	mu, ok := ls.l.MultipartUploads[uploadID]
	if ok {
		// fast path
		return mu, nil
	}
	data, err := ls.ds.Get(dsPartKey.ChildString(uploadID))
	if err == datastore.ErrNotFound {
		return nil, nil // not found is nil, nil as documented
	}
	if err != nil {
		return nil, err
	}
	mu = &MultipartUpload{}
	err = mu.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	// cache MultipartUpload
	ls.l.MultipartUploads[uploadID] = mu
	return mu, nil
}
