package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"ela-indexer/internal/config"
	"ela-indexer/internal/db"
	"ela-indexer/internal/mempool"
	"ela-indexer/internal/node"
	"ela-indexer/internal/rpc"
	eSync "ela-indexer/internal/sync"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("received %s, shutting down", sig)
		cancel()
	}()

	// Database
	database, err := db.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer database.Close()
	log.Printf("database connected")

	if err := database.SanityCheck(ctx); err != nil {
		log.Fatalf("startup sanity check failed: %v", err)
	}

	// ELA node client
	nodeClient := node.NewClient(cfg.ELANodeRPC)
	refClient := node.NewExternalClient(cfg.ReferenceRPC)

	// Verify node connectivity
	count, err := nodeClient.GetBlockCount(ctx)
	if err != nil {
		log.Fatalf("ela node unreachable: %v", err)
	}
	log.Printf("ela node connected, chain height: %d", count-1)

	// RPC server
	rpcServer := rpc.NewServer(database, nodeClient, nil)

	// Mempool index — polls node mempool, provides pending entries for gethistory
	mempoolIdx := mempool.NewIndex(nodeClient, database)
	rpcServer.SetMempool(mempoolIdx)
	go mempoolIdx.Run(ctx)

	// Syncer with cache invalidation callback wired to the RPC server
	syncer := eSync.NewSyncer(database, nodeClient, refClient, rpcServer.InvalidateCache)

	// Wire syncer into RPC server for health/status
	rpcServer.SetSyncer(syncer)

	// Start syncer in background
	go func() {
		if err := syncer.Start(ctx); err != nil {
			if ctx.Err() == nil {
				log.Fatalf("syncer: %v", err)
			}
		}
	}()

	// Start RPC server (blocks until context cancelled or error)
	go func() {
		if err := rpcServer.ListenAndServe(cfg.ListenAddr); err != nil {
			if ctx.Err() == nil {
				log.Fatalf("rpc server: %v", err)
			}
		}
	}()

	// Wait for shutdown signal
	<-ctx.Done()
	log.Printf("shutdown complete")
}
