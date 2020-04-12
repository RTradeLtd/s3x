package s3x

import (
	"context"
	"testing"

	"github.com/ipfs/go-datastore"

	dssync "github.com/ipfs/go-datastore/sync"
)

func TestS3X_LedgerStore_Badger(t *testing.T) {
	testS3XLedgerStore(t, DSTypeBadger)
}
func TestS3X_LedgerStore_Crdt(t *testing.T) {
	testS3XLedgerStore(t, DSTypeCrdt)
}
func testS3XLedgerStore(t *testing.T, dsType DSType) {
	ctx := context.Background()
	gateway := newTestGateway(t, dsType)
	defer func() {
		if err := gateway.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}()

	ledger, err := newLedgerStore(dssync.MutexWrap(datastore.NewMapDatastore()), gateway.dagClient)

	if err != nil {
		t.Fatal(err)
	}
	t.Run("create Buckets", func(t *testing.T) {
		tests := []struct {
			name             string
			bucket           string
			location         string
			wantCreateErr    bool
			locationExpected string
		}{
			{"add new bucket", "bucket1", "1", false, "1"},
			{"add existing bucket", "bucket1", "0", true, "1"},
			{"add bucket with different name", "bucket2", "2", false, "2"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := ledger.CreateBucket(ctx, tt.bucket, &Bucket{
					BucketInfo: BucketInfo{
						Location: tt.location,
					},
				})
				if (err != nil) != tt.wantCreateErr {
					t.Fatalf("NewBucket() err %v, wantErr %v", err, tt.wantCreateErr)
				}
				bi, err := ledger.GetBucketInfo(ctx, tt.bucket)
				if err != nil {
					t.Fatal(err)
				}
				name := bi.GetName()
				if name != tt.bucket {
					t.Fatalf("expected bucket name %v, but got %v", tt.bucket, name)
				}
				l := bi.GetLocation()
				if l != tt.locationExpected {
					t.Fatalf("expected location %v, but got %v", tt.locationExpected, l)
				}
			})
		}
	})
	t.Run("GetBucketNames", func(t *testing.T) {
		args := struct {
			wantLen     int
			wantBuckets []string
		}{2, []string{"bucket1", "bucket2"}}
		names, err := ledger.GetBucketNames()
		if err != nil {
			t.Fatal(err)
		}
		if len(names) != args.wantLen {
			t.Fatalf("bad number of buckets, got: %v", names)
		}
		var found1, found2 bool
		for _, name := range names {
			switch name {
			case "bucket1":
				found1 = true
			case "bucket2":
				found2 = true
			}
		}
		if !found1 || !found2 {
			t.Fatal("failed to find buckets")
		}
	})
}
