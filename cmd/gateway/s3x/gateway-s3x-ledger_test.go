package s3x

import (
	"testing"

	"github.com/ipfs/go-datastore"

	dssync "github.com/ipfs/go-datastore/sync"
)

func TestLedger(t *testing.T) {
	ledger, err := newLedgerStore(dssync.MutexWrap(datastore.NewMapDatastore()), nil) //todo: change test
	if err != nil {
		t.Fatal(err)
	}
	type args struct {
		name, hash string
	}
	t.Run("NewBucket", func(t *testing.T) {
		tests := []struct {
			name    string
			args    args
			wantErr bool
		}{
			{"1", args{"testbucket", "hash1"}, false},
			{"2", args{"testbucket", "hash2"}, true},
			{"3", args{"testbucket3", "hash3"}, false},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := ledger.NewBucket(tt.args.name, tt.args.hash)
				if (err != nil) != tt.wantErr {
					t.Fatalf("NewBucket() err %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	})
	t.Run("GetBucketHash", func(t *testing.T) {
		tests := []struct {
			name     string
			args     args
			wantHash string
			wantErr  bool
		}{
			{"1", args{"testbucket", ""}, "hash1", false},
			{"2", args{"testbucket99999", ""}, "hash2", true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				b, err := ledger.getBucket(tt.args.name)
				if (err != nil) != tt.wantErr {
					t.Fatalf("GetBucketHash() err %v, wantErr %v", err, tt.wantErr)
				}
				if err == nil && b.GetIpfsHash() != tt.wantHash {
					t.Fatal("bad bucket hash returned")
				}
			})
		}
	})
	t.Run("GetBucketNames", func(t *testing.T) {
		args := struct {
			wantLen     int
			wantBuckets []string
		}{2, []string{"testbucket", "testbucket3"}}
		names, err := ledger.GetBucketNames()
		if err != nil {
			t.Fatal(err)
		}
		if len(names) != args.wantLen {
			t.Fatal("bad number of buckets")
		}
		var found1, found3 bool
		for _, name := range names {
			switch name {
			case "testbucket":
				found1 = true
			case "testbucket3":
				found3 = true
			}
		}
		if !found1 || !found3 {
			t.Fatal("failed to find buckets")
		}
	})
}
