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
	ls := gateway.ledgerStore
	root := ls.refIDRoot
	if len(root.String()) < 20 {
		t.Fatalf("refIDRoot %v is too short", root)
	}
	if err := ls.setRefIDRoot(gateway.temx); err != nil {
		t.Fatalf("setRefIDRoot should no return error when recalled: %v", err)
	}
	if root != ls.refIDRoot {
		t.Fatalf("refIDRoot changed from %v to %v", root, ls.refIDRoot)
	}

	ra := ls.objectRefID("a/b", "c")
	rb := ls.objectRefID("a", "b/c")
	if ra == rb {
		t.Fatalf("RefID %v must be different from %v", ra, rb)
	}
	gateway.restart(t)
	ls = gateway.ledgerStore
	ra2 := ls.objectRefID("a/b", "c")
	if ra != ra2 {
		t.Fatalf("RefID must be the same after a restart %v != %v", ra, ra2)
	}
	gateway.temx.SFSName = "fake-server"
	if err := ls.setRefIDRoot(gateway.temx); err != nil {
		t.Fatal(err)
	}
	if root == ls.refIDRoot {
		t.Fatalf("refIDRoot must change when SFSName changes")
	}
	gateway.temx.SFSName = ""
	if err := ls.setRefIDRoot(gateway.temx); err != nil {
		t.Fatal(err)
	}
	if root != ls.refIDRoot {
		t.Fatalf("refIDRoot must change back when SFSName changes back %v != %v", root, ls.refIDRoot)
	}
}
