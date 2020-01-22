package s3x

import (
	"bytes"
	"context"
	"testing"

	"github.com/ipfs/go-datastore"

	dssync "github.com/ipfs/go-datastore/sync"
)

func TestLedgerStore(t *testing.T) {
	ctx := context.Background()
	gateway := getTestGateway(t)
	defer func() { _ = gateway.Shutdown(ctx) }()

	ledger, err := newLedgerStore(dssync.MutexWrap(datastore.NewMapDatastore()), gateway.(*xObjects).dagClient)

	if err != nil {
		t.Fatal(err)
	}
	t.Run("create Buckets", func(t *testing.T) {
		tests := []struct {
			name          string
			bucket        string
			data          []byte
			wantCreateErr bool
			dataExpected  []byte
		}{
			{"add new bucket", "bucket1", []byte{1}, false, []byte{1}},
			{"add existing bucket", "bucket1", []byte{0}, true, []byte{1}},
			{"add bucket with different name", "bucket2", []byte{2}, false, []byte{2}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := ledger.createBucket(ctx, tt.bucket, &Bucket{
					Data: tt.data,
				})
				if (err != nil) != tt.wantCreateErr {
					t.Fatalf("NewBucket() err %v, wantErr %v", err, tt.wantCreateErr)
				}
				le, err := ledger.getBucketLoaded(ctx, tt.bucket)
				if err != nil {
					t.Fatal(err)
				}
				d := le.GetBucket().GetData()
				if !bytes.Equal(d, tt.dataExpected) {
					t.Fatalf("expected %v, but got %v", tt.dataExpected, d)
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
