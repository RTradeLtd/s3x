package s3x

import (
	"crypto/rand"
	"crypto/sha256"

	"github.com/ipfs/go-datastore"
	"github.com/pkg/errors"
)

//refID is used to tag resources for future deletion in shared storage
type refID string

func (ls *ledgerStore) setRefIDRoot(g *TEMX) error {
	key := datastore.NewKey("rtrade/s3x/RefIDSecret")
	v, err := ls.ds.Get(key)
	if err == datastore.ErrNotFound {
		v = make([]byte, 64)
		if _, err = rand.Read(v); err != nil {
			return err
		}
		err = ls.ds.Put(key, v)
	}
	if err != nil {
		return err
	}
	if len(v) < 10 {
		return errors.Errorf(`the RefIDSecret "%x" is not secure`, v)
	}
	h := sha256.New()
	if _, err = h.Write(v); err != nil {
		return err
	}
	if _, err = h.Write([]byte(g.SFSName)); err != nil {
		return err
	}
	ls.refIDRoot = string(h.Sum(nil))
	return nil
}
