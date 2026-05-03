package config

import (
	"fmt"
	"os"
)

type Config struct {
	DatabaseURL  string
	ELANodeRPC   string
	ListenAddr   string
	ReferenceRPC string // for reconciliation + proposal titles, defaults to api.elastos.io
}

func Load() (Config, error) {
	c := Config{
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		ELANodeRPC:   os.Getenv("ELA_NODE_RPC"),
		ListenAddr:   os.Getenv("LISTEN_ADDR"),
		ReferenceRPC: os.Getenv("REFERENCE_RPC"),
	}
	if c.DatabaseURL == "" {
		return c, fmt.Errorf("DATABASE_URL is required")
	}
	if c.ELANodeRPC == "" {
		return c, fmt.Errorf("ELA_NODE_RPC is required")
	}
	if c.ListenAddr == "" {
		c.ListenAddr = "127.0.0.1:8337"
	}
	if c.ReferenceRPC == "" {
		c.ReferenceRPC = "https://api.elastos.io/ela"
	}
	return c, nil
}
