// Package etcd implements a confloader client backed by go.etcd.io/etcd/client/v3.
package etcd

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/client/v3"

	"github.com/fikrimohammad/go-dev-sdk/confloader/client"
)

// Client implements client.Client against etcd.
//
// Storage mapping (standardized model):
//
//	namespace -> root key prefix:        <namespace>/
//	folder    -> one level below root:   <namespace>/<folder>/
//	key       -> leaf key:               <namespace>/<folder>/<key>
//
// A key may never live directly under the root, and folders have no
// subfolders, so the path is exactly three segments.
type Client struct {
	cli       *clientv3.Client
	namespace string
}

// Config configures the etcd client connection.
type Config struct {
	Endpoint         string
	AuthClientID     string
	AuthClientSecret string
	Namespace        string
}

// New builds an etcd-backed client. Authentication uses the static
// username/password credentials.
func New(cfg Config) (*Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{cfg.Endpoint},
		Username:    cfg.AuthClientID,
		Password:    cfg.AuthClientSecret,
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("confloader/etcd: connect: %w", err)
	}
	return &Client{cli: cli, namespace: cfg.Namespace}, nil
}

// keyPath builds the full etcd key for a folder/key under the namespace root.
func (c *Client) keyPath(folder, key string) string {
	return c.namespace + "/" + folder + "/" + key
}

func (c *Client) Fetch(ctx context.Context, folder, key string) (client.Fetched, error) {
	if folder == "" {
		return client.Fetched{}, fmt.Errorf("confloader/etcd: empty folder (config cannot reside in root)")
	}
	resp, err := c.cli.Get(ctx, c.keyPath(folder, key))
	if err != nil {
		return client.Fetched{}, fmt.Errorf("confloader/etcd: fetch: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return client.Fetched{}, client.ErrNotFound
	}
	kv := resp.Kvs[0]
	return client.Fetched{
		Value:    string(kv.Value),
		Revision: fmt.Sprintf("%d", kv.ModRevision),
	}, nil
}

func (c *Client) Close() error {
	return c.cli.Close()
}
