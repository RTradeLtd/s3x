package s3x

import (
	"context"
	"reflect"
	"testing"
)

func Test_xObjects_GetHash(t *testing.T) {
	tests := []struct {
		name    string
		req     *InfoRequest
		want    *InfoResponse
		wantErr bool
	}{
		{"bucket hash", &InfoRequest{Bucket: testBucket1}, &InfoResponse{Bucket: testBucket1, Hash: "bafkreibcp6rasofwb54zyx5np2hu272ujeamrzio5zhgt7szfsriug4ypq"}, false},
		{"object hash", &InfoRequest{Bucket: testBucket1, Object: testObject1}, &InfoResponse{Bucket: testBucket1, Object: testObject1, Hash: "bafkreicqrnvazls3q7j44gneiuajfuw7b3tk7d4f6r5wvgldywoq4otg7y"}, false},
		{"object data hash", &InfoRequest{Bucket: testBucket1, Object: testObject1, ObjectDataOnly: true}, &InfoResponse{Bucket: testBucket1, Object: testObject1, Hash: "bafkreial77jjylrtb2na7bmhnhvzrjnbxw77twwtbqblcpj5rkw6fg2qwm"}, false},
	}

	ctx := context.Background()
	gateway := getTestGateway(t)
	defer func() {
		if err := gateway.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	if err := gateway.MakeBucketWithLocation(ctx, testBucket1, "us-east-1"); err != nil {
		t.Fatal(err)
	}
	testPutObject(t, gateway)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := gateway.GetHash(ctx, tt.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("xObjects.GetHash() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("xObjects.GetHash() = %v, want %v", got, tt.want)
			}
		})
	}
}
