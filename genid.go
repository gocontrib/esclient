package esclient

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
	"sync/atomic"
)

var newID = makeIDGenerator()

func makeIDGenerator() func() string {
	var buf [12]byte
	var b64 string
	for len(b64) < 10 {
		rand.Read(buf[:])
		b64 = base64.StdEncoding.EncodeToString(buf[:])
		b64 = strings.NewReplacer("+", "", "/", "").Replace(b64)
	}
	prefix := fmt.Sprintf("%s", b64[0:10])
	var counter uint64
	return func() string {
		id := atomic.AddUint64(&counter, 1)
		return fmt.Sprintf("%s-%06d", prefix, id)
	}
}

func NewID() string {
	return newID()
}
