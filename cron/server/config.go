package server

import "github.com/fikrimohammad/go-dev-sdk/apiserver"

// Config holds the server configuration, embedding apiserver.Config.
type Config struct {
	apiserver.Config `yaml:",inline" json:",inline"`
	Prefix           string `yaml:"prefix" json:"prefix"`
}
