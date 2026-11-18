package blocksapi

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pb "github.com/aurora-is-near/borealis-prototypes-go/generated/blocksapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"s3-migrate/config"
)

// ErrNotFound is returned when the stream has no message at that height.
var ErrNotFound = errors.New("blocks-api: block not found")

// Client fetches one NEAR block document from Blocks API.
type Client struct {
	conn   *grpc.ClientConn
	rpc    pb.BlocksProviderClient
	token  string
	stream string
	origin string
}

// Dial opens a Blocks API connection. The caller must Close it.
func Dial(cfg config.BlocksAPIConfig) (*Client, error) {
	endpoint := strings.TrimSpace(cfg.Endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("blocks_api.endpoint is empty")
	}
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, fmt.Errorf("blocks_api.token is empty")
	}
	if strings.TrimSpace(cfg.Stream) == "" {
		return nil, fmt.Errorf("blocks_api.stream is empty")
	}
	conn, err := grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithInitialConnWindowSize(64*1024*1024),
		grpc.WithInitialWindowSize(64*1024*1024),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(1<<30),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("blocks-api dial %s: %w", endpoint, err)
	}
	return &Client{
		conn:   conn,
		rpc:    pb.NewBlocksProviderClient(conn),
		token:  cfg.Token,
		stream: cfg.Stream,
		origin: cfg.Origin,
	}, nil
}

// Close releases the gRPC connection.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// FetchBlockJSON returns the neardata-shaped JSON document for height.
func (c *Client) FetchBlockJSON(ctx context.Context, height int64) ([]byte, error) {
	if height < 0 {
		return nil, fmt.Errorf("blocks-api: negative height %d", height)
	}
	format := pb.BlockMessage_PAYLOAD_NEAR_BLOCK_V2
	req := &pb.GetBlockMessageRequest{
		StreamName:   c.stream,
		StreamOrigin: c.origin,
		MessageId: &pb.BlockMessage_ID{
			Kind:   pb.BlockMessage_MSG_WHOLE,
			Height: uint64(height),
		},
		DeliverySettings: &pb.BlockMessageDeliverySettings{
			RequireFormat: &format,
		},
	}
	callCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+c.token))
	resp, err := c.rpc.GetBlockMessage(callCtx, req)
	if err != nil {
		return nil, fmt.Errorf("blocks-api GetBlockMessage height %d: %w", height, err)
	}
	if errResp := resp.GetError(); errResp != nil {
		switch errResp.Kind {
		case pb.GetBlockMessageResponse_Error_NOT_FOUND,
			pb.GetBlockMessageResponse_Error_LOW_ID,
			pb.GetBlockMessageResponse_Error_HIGH_ID:
			return nil, fmt.Errorf("blocks-api height %d: %w", height, ErrNotFound)
		default:
			return nil, fmt.Errorf("blocks-api height %d: %s: %s", height, errResp.Kind.String(), errResp.Description)
		}
	}
	msg := resp.GetResult().GetMessage()
	if msg == nil {
		return nil, fmt.Errorf("blocks-api height %d: empty result", height)
	}
	if msg.GetCompression() != pb.BlockMessage_COMPRESSION_NONE {
		return nil, fmt.Errorf("blocks-api height %d: unsupported compression %s", height, msg.GetCompression().String())
	}
	raw := msg.GetRawPayload()
	if len(raw) == 0 {
		return nil, fmt.Errorf("blocks-api height %d: empty payload", height)
	}
	if msg.GetFormat() != pb.BlockMessage_PAYLOAD_NEAR_BLOCK_V2 {
		return nil, fmt.Errorf("blocks-api height %d: unexpected format %s", height, msg.GetFormat().String())
	}
	body, err := decodeNearBlockJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("blocks-api height %d: %w", height, err)
	}
	return body, nil
}
