package blocksapi

import (
	"bytes"
	"fmt"
	"io"

	"github.com/fxamacker/cbor/v2"
	"github.com/pierrec/lz4/v4"
)

// borealisEnvelope is the version-1 header that precedes a NEAR block payload.
// Field order matches stream-most's CBOR array encoding.
type borealisEnvelope struct {
	_            struct{} `cbor:",toarray"`
	Type         uint16
	SequentialID uint64
	TimestampS   uint32
	TimestampMS  uint16
	UniqueID     [16]byte
}

var cborDecMode cbor.DecMode

func init() {
	var err error
	cborDecMode, err = cbor.DecOptions{
		MaxArrayElements: 2147483647,
	}.DecMode()
	if err != nil {
		panic(fmt.Errorf("blocks-api cbor decoder: %w", err))
	}
}

// decodeNearBlockJSON unwraps a PAYLOAD_NEAR_BLOCK_V2 blob into the neardata
// document shape: {"block":..., "shards":[...]}.
func decodeNearBlockJSON(data []byte) ([]byte, error) {
	compressed, err := decodeBorealisBytes(data)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, lz4.NewReader(bytes.NewReader(compressed))); err != nil {
		return nil, fmt.Errorf("lz4: %w", err)
	}
	if !jsonObject(buf.Bytes()) {
		return nil, fmt.Errorf("decoded payload is not a JSON object")
	}
	return buf.Bytes(), nil
}

func decodeBorealisBytes(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty borealis payload")
	}
	reader := bytes.NewReader(data)
	version, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if version != 1 {
		return nil, fmt.Errorf("unknown borealis version %d", version)
	}
	decoder := cborDecMode.NewDecoder(reader)
	if err := decoder.Decode(&borealisEnvelope{}); err != nil {
		return nil, fmt.Errorf("borealis envelope: %w", err)
	}
	var payload []byte
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("borealis payload: %w", err)
	}
	return payload, nil
}

func jsonObject(b []byte) bool {
	for _, c := range b {
		if c == ' ' || c == '\n' || c == '\r' || c == '\t' {
			continue
		}
		return c == '{'
	}
	return false
}
