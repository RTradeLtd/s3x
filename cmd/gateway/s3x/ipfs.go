package s3x

import (
	"context"
	"fmt"
	"io"

	pb "github.com/RTradeLtd/TxPB/v3/go"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
	"github.com/pkg/errors"
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
		return "", errors.Wrap(err, "dag client error in ipfsSaveBytes")
	}
	return resp.GetHashes()[0], nil
}

func ipfsSaveProtoNode(ctx context.Context, dag pb.NodeAPIClient, node *merkledag.ProtoNode) (string, error) {
	data, err := node.Marshal()
	if err != nil {
		return "", err
	}
	resp, err := dag.Dag(ctx, &pb.DagRequest{
		RequestType:         pb.DAGREQTYPE_DAG_PUT,
		Data:                data,
		ObjectEncoding:      "protobuf",
		SerializationFormat: "protobuf",
	})
	if err != nil {
		return "", errors.Wrap(err, "dag client error in ipfsSaveProtoNode")
	}
	if len(resp.GetHashes()) != 1 {
		return "", errors.New("unexpected number of hashes returned")
	}
	return resp.GetHashes()[0], nil
}

const chunkSize = 4*1024*1024 - 1024 //1KB less than 4MB for a good safety buffer

func ipfsFileUpload(ctx context.Context, fileClient pb.FileAPIClient, r io.Reader) (string, int, error) {
	stream, err := fileClient.UploadFile(ctx)
	if err != nil {
		return "", 0, err
	}
	var (
		buf  = make([]byte, chunkSize)
		size int
	)
	for {
		n, err := r.Read(buf)
		if err == io.EOF {
			if n == 0 {
				break
			}
		} else if err != nil {
			_ = stream.CloseSend()
			return "", size, err
		}
		size = size + n
		if err := stream.Send(&pb.UploadRequest{
			Blob: &pb.Blob{Content: buf[:n]},
		}); err != nil {
			return "", size, err
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return "", size, err
	}
	if _, err := cid.Decode(resp.Hash); err != nil {
		return "", size, fmt.Errorf("resp.Hash mush be a valid cid, but got error: %v", err)
	}
	return resp.Hash, size, nil
}

func ipfsFileDownload(ctx context.Context, fileClient pb.FileAPIClient, w io.Writer, hash string, startOffset, length int64) (int64, error) {
	isSubSet := startOffset != 0 || length != 0
	//TODO: put startOffset and length in DownloadRequest to improve performance
	stream, err := fileClient.DownloadFile(ctx, &pb.DownloadRequest{
		Hash:      hash,
		ChunkSize: chunkSize, //TODO: determine an optimal size
	})
	var n int64
	if err != nil {
		return n, err
	}
	for {
		recv, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return n, nil
			}
			return n, err
		}
		data := recv.GetBlob().GetContent()
		if isSubSet {
			if int64(len(data)) < startOffset {
				startOffset -= int64(len(data))
				continue
			}
			if startOffset > 0 {
				data = data[startOffset:]
				startOffset = 0
			}
			if int64(len(data)) > length {
				data = data[:length]
				length = 0
			} else {
				length -= int64(len(data))
			}
		}
		m, err := w.Write(data)
		n += int64(m)
		if err != nil {
			_ = stream.CloseSend()
			return n, err
		}
		if isSubSet && length == 0 {
			_ = stream.CloseSend()
			return n, nil
		}
	}
}
