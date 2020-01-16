package s3x

import (
	"context"

	pb "github.com/RTradeLtd/TxPB/v3/go"
)

type unmarshaller interface {
	Unmarshal(data []byte) error
}
type marshaller interface {
	Marshal() ([]byte, error)
}

// ipfsBytes returns data from IPFS using its hash
func ipfsBytes(ctx context.Context, dag pb.NodeAPIClient, h string) ([]byte, error) {
	resp, err := dag.Dag(ctx, &pb.DagRequest{
		RequestType: pb.DAGREQTYPE_DAG_GET,
		Hash:        h,
	})
	return resp.GetRawData(), err
}

// ipfsUnmarshal unmarshalls any data structure from IPFS using its hash
func ipfsUnmarshal(ctx context.Context, dag pb.NodeAPIClient, h string, u unmarshaller) error {
	data, err := ipfsBytes(ctx, dag, h)
	if err != nil {
		return err
	}
	return u.Unmarshal(data)
}

// ipfsObject returns an object from IPFS using its hash
func ipfsObject(ctx context.Context, dag pb.NodeAPIClient, h string) (*Object, error) {
	obj := &Object{}
	if err := ipfsUnmarshal(ctx, dag, h, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// ipfsBucket returns a bucket from IPFS using its hash
func ipfsBucket(ctx context.Context, dag pb.NodeAPIClient, h string) (*Bucket, error) {
	b := &Bucket{}
	if err := ipfsUnmarshal(ctx, dag, h, b); err != nil {
		return nil, err
	}
	return b, nil
}

// ipfsSave saves any marshaller object and returns it's IPFS hash
func ipfsSave(ctx context.Context, dag pb.NodeAPIClient, m marshaller) (string, error) {
	data, err := m.Marshal()
	if err != nil {
		return "", err
	}
	return ipfsSaveBytes(ctx, dag, data)
}

// ipfsSaveBytes saves data and returns it's IPFS hash
func ipfsSaveBytes(ctx context.Context, dag pb.NodeAPIClient, data []byte) (string, error) {
	resp, err := dag.Dag(ctx, &pb.DagRequest{
		RequestType: pb.DAGREQTYPE_DAG_PUT,
		Data:        data,
	})
	if err != nil {
		return "", err
	}
	return resp.GetHashes()[0], nil
}
