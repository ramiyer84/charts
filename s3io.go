package s3io

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type Client struct {
	s3     *s3.Client
	Bucket string
	Prefix string
}

func New(ctx context.Context, region, bucket, prefix string, opts ...func(*config.LoadOptions) error) (*Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, append(opts, config.WithRegion(region))...)
	if err != nil {
		return nil, err
	}
	return &Client{s3: s3.NewFromConfig(cfg), Bucket: bucket, Prefix: prefix}, nil
}

// NotFoundError is exported so other packages can detect “missing object”
// WITHOUT importing AWS packages.
type NotFoundError struct{ Key string }
func (e *NotFoundError) Error() string { return "s3 object not found: " + e.Key }

func (c *Client) Key(name string) string {
	if c.Prefix == "" {
		return name
	}
	return c.Prefix + name
}

func (c *Client) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// Normalize AWS errors to our NotFoundError
		var nsk *types.NoSuchKey
		if as := errorAs(err, &nsk); as {
			return nil, &NotFoundError{Key: key}
		}
		var api smithy.APIError
		if as := errorAs(err, &api); as && api.ErrorCode() == "NoSuchKey" {
			return nil, &NotFoundError{Key: key}
		}
		return nil, err
	}
	return out.Body, nil
}

// tiny helper so we don’t pull "errors" into every callsite
func errorAs(err error, target any) bool { return smithy.As(err, target) }