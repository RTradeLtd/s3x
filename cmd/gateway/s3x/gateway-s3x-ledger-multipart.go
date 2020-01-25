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
func (ls *ledgerStore) AbortMultipartUpload(bucket, multipartID string) error {
	ex, err := ls.bucketExists(bucket)
	if err != nil {
		return err
	}
	if !ex {
		return ErrLedgerBucketDoesNotExist
	}
	if err := ls.l.multipartExists(multipartID); err != nil {
		return err
	}
	return ls.l.deleteMultipartID(bucket, multipartID)
}

// NewMultipartUpload is used to store the initial start of a multipart upload request
func (ls *ledgerStore) NewMultipartUpload(multipartID string, info *ObjectInfo) error {
	bucket := info.GetBucket()
	defer ls.locker.write(bucket)
	err := ls.assertBucketExits(bucket)
	if err != nil {
		return err
	}
	if ls.l.MultipartUploads == nil {
		ls.l.MultipartUploads = make(map[string]*MultipartUpload)
	}
	ls.l.MultipartUploads[multipartID] = &MultipartUpload{
		ObjectInfo: info,
		Id:         multipartID,
	}
	return nil //todo: save to ipfs
}

// PutObjectPart is used to record an individual object part within a multipart upload
func (ls *ledgerStore) PutObjectPart(bucketName, objectName, partHash, multipartID string, partNumber int64) error {
	err := ls.assertBucketExits(bucketName)
	if err != nil {
		return err
	}
	mpart, ok := ls.l.MultipartUploads[multipartID]
	if !ok {
		return ErrInvalidUploadID
	}
	mpart.ObjectParts[partHash] = ObjectPartInfo{
		Number:   partNumber,
		DataHash: partHash,
	}
	return nil //todo: save to ipfs
}

/////////////////////
// GETTER FUNCTINS //
/////////////////////

// GetObjectParts is used to return multipart upload parts
func (ls *ledgerStore) GetObjectParts(id string) (map[string]ObjectPartInfo, error) {
	if err := ls.l.multipartExists(id); err != nil {
		return nil, err
	}
	return ls.l.GetMultipartUploads()[id].ObjectParts, nil
}

// MultipartIDExists is used to lookup if the given multipart id exists
func (ls *ledgerStore) MultipartIDExists(id string) error {
	return ls.l.multipartExists(id)
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

func (m *Ledger) deleteMultipartID(bucketName, multipartID string) error {
	delete(m.MultipartUploads, multipartID)
	//todo: save to ipfs
	return nil
}
