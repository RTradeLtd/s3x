package s3x

import (
	"context"
	"testing"
)

func TestSetRefIDRoot(t *testing.T) {
	ctx := context.Background()
	gateway := newTestGateway(t, DSTypeBadger, false)
	defer func() {
		if err := gateway.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	root := gateway.ledgerStore.refIDRoot
	if len(root) < 20 {
		t.Fatalf("refIDRoot %v is too short", root)
	}
	if err := gateway.ledgerStore.setRefIDRoot(gateway.temx); err != nil {
		t.Fatalf("setRefIDRoot should no return error when recalled: %v", err)
	}
	if root != gateway.ledgerStore.refIDRoot {
		t.Fatalf("refIDRoot changed from %v to %v", root, gateway.ledgerStore.refIDRoot)
	}
}
