// Package infisical implements a confloader client backed by the Infisical Go SDK.
package infisical

import (
	"context"
	"errors"
	"fmt"

	infisical "github.com/infisical/go-sdk"
	sdkerrors "github.com/infisical/go-sdk/packages/errors"

	"github.com/fikrimohammad/go-dev-sdk/confloader/client"
)

// Client implements client.Client against Infisical.
//
// Storage mapping (standardized model):
//
//	namespace  -> Infisical workspace/project ID
//	folder     -> secret path: /<folder>            (root folder == "/")
//	key        -> secret key
//
// Infisical requires a workspace + environment + secret path. The environment
// comes from cfg.Environment. Folders are expressed as a secret path of the
// form /<folder>; the root folder and the "default" folder map to "/".
type Client struct {
	client      infisical.InfisicalClientInterface
	namespace   string
	environment string
}

// Config configures the Infisical client connection.
type Config struct {
	Endpoint         string
	AuthClientID     string
	AuthClientSecret string
	Namespace        string
	Environment      string
}

// New builds an Infisical-backed client and logs in with Universal Auth.
func New(cfg Config) (*Client, error) {
	c := infisical.NewInfisicalClient(context.Background(), infisical.Config{
		SiteUrl: cfg.Endpoint,
	})
	if _, err := c.Auth().UniversalAuthLogin(cfg.AuthClientID, cfg.AuthClientSecret); err != nil {
		return nil, fmt.Errorf("confloader/infisical: login: %w", err)
	}
	return &Client{
		client:      c,
		namespace:   cfg.Namespace,
		environment: cfg.Environment,
	}, nil
}

// secretPath maps a folder name to an Infisical secret path. The root folder
// and the "default" folder are represented as "/", while any other folder is
// "/<folder>".
func secretPath(folder string) string {
	switch folder {
	case "", client.DefaultFolder:
		return "/"
	default:
		return "/" + folder
	}
}

func (c *Client) Fetch(ctx context.Context, folder, key string) (client.Fetched, error) {
	secret, err := c.client.Secrets().Retrieve(infisical.RetrieveSecretOptions{
		SecretKey:   key,
		ProjectID:   c.namespace,
		Environment: c.environment,
		SecretPath:  secretPath(folder),
	})
	if err != nil {
		if isNotFound(err) {
			return client.Fetched{}, client.ErrNotFound
		}
		return client.Fetched{}, fmt.Errorf("confloader/infisical: fetch: %w", err)
	}
	// Infisical secrets have no per-value ETag in the Retrieve response, so the
	// value itself is used as the revision; a change is still detected.
	return client.Fetched{
		Value:    secret.SecretValue,
		Revision: fmt.Sprintf("%d", secret.Version),
	}, nil
}

func (c *Client) Close() error {
	return nil
}

// isNotFound reports whether err means the secret does not exist (HTTP 404).
func isNotFound(err error) bool {
	var apiErr *sdkerrors.APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode == 404
	}
	return false
}
