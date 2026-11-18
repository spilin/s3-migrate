package blocksapi

import (
	"bytes"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/pierrec/lz4/v4"
)

func TestDecodeNearBlockJSON(t *testing.T) {
	raw, err := encodeNearBlockJSON([]byte(`{"block":{"header":{"height":7}},"shards":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeNearBlockJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"block":{"header":{"height":7}},"shards":[]}` {
		t.Fatalf("decoded %s", got)
	}
}

func encodeNearBlockJSON(doc []byte) ([]byte, error) {
	var compressed bytes.Buffer
	w := lz4.NewWriter(&compressed)
	if _, err := w.Write(doc); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	env, err := cbor.Marshal(&borealisEnvelope{})
	if err != nil {
		return nil, err
	}
	payload, err := cbor.Marshal(compressed.Bytes())
	if err != nil {
		return nil, err
	}
	out := append([]byte{1}, env...)
	return append(out, payload...), nil
}
