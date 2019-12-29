package s3x

import (
	"context"

	pb "github.com/RTradeLtd/TxPB/go"
)

func (x *xObjects) getObjectData(
	ctx context.Context,
	obj *Object,
) ([]byte, error) {
	resp, err := x.dagClient.DagGet(ctx, &pb.DagGetRequest{
		Hash: obj.DataHash,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetRawData(), nil
}

// store the objectData on ipfs and returns its hash
func (x *xObjects) storeObjectData(
	ctx context.Context,
	data []byte,
) (string, error) {
	resp, err := x.dagClient.DagPut(ctx, &pb.DagPutRequest{Data: data})
	if err != nil {
		return "", err
	}
	return resp.GetHashes()[0], nil
}
