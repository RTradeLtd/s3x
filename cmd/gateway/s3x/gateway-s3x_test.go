package s3x

import (
	"net"
	"strings"
	"testing"
	"time"
)

// testDial blocks until all available ports are free
func testDial(t *testing.T) {
	canDial(t, "localhost:8888")
	canDial(t, "localhost:8889")
}

// canDial is allows us to prevent fully starting a test
// until we determine that an address is available for listening
func canDial(t *testing.T, listenAddress string) bool {
	count := 1
	for {
		if handler, err := net.Listen("tcp", listenAddress); err == nil {
			if err := handler.Close(); err != nil {
				t.Fatal(err)
			}
			return true
		} else if err != nil {
			if strings.Contains(err.Error(), "already in use") {
				time.Sleep(time.Second * time.Duration(count))
			}
		}
		count = count + 1
	}
}
