package s3x

import (
	"context"

	pb "github.com/RTradeLtd/TxPB/v3/go"
)

//crdtBroadcaster implements crdt.Broadcaster using a pb.PubSubAPIClient
type crdtBroadcaster struct {
	topic  string
	client pb.PubSubAPI_PubSubClient
	next   chan []byte
	err    error //only read from if next is closed
}

// newCrdtBroadcaster builds a crdtBroadcaster, ctx must be closed after use to release resources.
func newCrdtBroadcaster(ctx context.Context, api pb.PubSubAPIClient, topic string) (*crdtBroadcaster, error) {
	client, err := api.PubSub(ctx)
	if err != nil {
		return nil, err
	}
	if err := client.Send(&pb.PubSubRequest{
		RequestType: pb.PSREQTYPE_PS_SUBSCRIBE,
		Topics:      []string{topic},
	}); err != nil {
		return nil, err
	}
	next := make(chan []byte)
	b := &crdtBroadcaster{
		topic:  topic,
		client: client,
		next:   next,
	}
	go func() {
		for {
			resp, err := client.Recv()
			if err != nil {
				b.err = err
				close(next)
				return
			}
			for _, m := range resp.GetMessage() {
				next <- m.GetData()
			}
		}
	}()
	return b, nil
}

// Broadcast sends payload to other replicas.
func (b *crdtBroadcaster) Broadcast(data []byte) error {
	return b.client.Send(&pb.PubSubRequest{
		RequestType: pb.PSREQTYPE_PS_PUBLISH,
		Topics:      []string{b.topic},
		Data:        data,
	})
}

// Next obtains the next payload received from the network.
func (b *crdtBroadcaster) Next() ([]byte, error) {
	data, ok := <-b.next
	if !ok {
		return nil, b.err
	}
	return data, nil
}
