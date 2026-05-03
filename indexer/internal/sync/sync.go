package sync

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	gosync "sync"
	"sync/atomic"
	"time"

	"ela-indexer/internal/db"
	"ela-indexer/internal/node"
	"ela-indexer/internal/validate"
)

const (
	pollInterval     = 2 * time.Second
	prefetchWorkers  = 4
	prefetchBuffer   = 8
	bulkBatchSize    = 50 // blocks per COPY batch during initial sync
	progressInterval = 100
)

type Syncer struct {
	db        *db.DB
	node      *node.Client
	refNode   *node.Client // reference endpoint for proposal titles
	height    atomic.Int64
	synced    atomic.Bool
	onNewBlock func() // callback to invalidate cache
}

func NewSyncer(database *db.DB, nodeClient *node.Client, refClient *node.Client, onNewBlock func()) *Syncer {
	return &Syncer{
		db:         database,
		node:       nodeClient,
		refNode:    refClient,
		onNewBlock: onNewBlock,
	}
}

func (s *Syncer) Height() int64   { return s.height.Load() }
func (s *Syncer) IsSynced() bool  { return s.synced.Load() }

func (s *Syncer) Start(ctx context.Context) error {
	lastHeightStr, err := s.db.GetSyncState(ctx, "last_height")
	if err != nil {
		return fmt.Errorf("get last_height: %w", err)
	}
	var startHeight int64
	if lastHeightStr != "" {
		startHeight, _ = strconv.ParseInt(lastHeightStr, 10, 64)
		s.height.Store(startHeight)

		lastHash, _ := s.db.GetSyncState(ctx, "last_hash")
		if lastHash != "" {
			if err := s.verifyChainTip(ctx, startHeight, lastHash); err != nil {
				return err
			}
			startHeight = s.height.Load()
		}
	}

	chainHeight, err := s.node.GetBlockCount(ctx)
	if err != nil {
		return fmt.Errorf("get chain height: %w", err)
	}
	chainHeight-- // getblockcount returns count, last block = count-1

	if startHeight < chainHeight-1 {
		log.Printf("[sync] initial sync from %d to %d (%d blocks)", startHeight+1, chainHeight, chainHeight-startHeight)
		if err := s.initialSync(ctx, startHeight+1, chainHeight); err != nil {
			return fmt.Errorf("initial sync: %w", err)
		}
	}

	s.synced.Store(true)
	log.Printf("[sync] caught up, entering live mode at height %d", s.height.Load())
	return s.liveSync(ctx)
}

func (s *Syncer) verifyChainTip(ctx context.Context, height int64, expectedHash string) error {
	block, err := s.node.GetBlockByHeight(ctx, height)
	if err != nil {
		return fmt.Errorf("verify tip at %d: %w", height, err)
	}
	if block.Hash == expectedHash {
		return nil
	}

	log.Printf("[sync] chain tip mismatch at %d: expected %s, got %s — rolling back",
		height, truncHash(expectedHash), truncHash(block.Hash))

	if err := s.db.RollbackBlock(ctx, height); err != nil {
		return fmt.Errorf("rollback block %d: %w", height, err)
	}
	s.height.Store(height - 1)

	if height > 1 {
		// Fetch parent block from node to get correct hash for new tip
		parentBlock, err := s.node.GetBlockByHeight(ctx, height-1)
		if err != nil {
			return fmt.Errorf("fetch parent block %d: %w", height-1, err)
		}
		// Update last_hash to parent's actual hash
		if err := s.db.SetSyncState(ctx, "last_hash", parentBlock.Hash); err != nil {
			return fmt.Errorf("update last_hash after rollback: %w", err)
		}
		return s.verifyChainTip(ctx, height-1, parentBlock.Hash)
	}
	return nil
}

// initialSync uses parallel block fetching + COPY for bulk loading.
func (s *Syncer) initialSync(ctx context.Context, from, to int64) error {
	log.Printf("[sync] dropping secondary indexes for bulk load")
	if err := s.db.DropSecondaryIndexes(ctx); err != nil {
		log.Printf("[sync] warning: drop indexes: %v", err)
	}

	type fetchResult struct {
		height int64
		block  *node.Block
		err    error
	}

	heightCh := make(chan int64, prefetchBuffer)
	resultCh := make(chan fetchResult, prefetchBuffer)

	var wg gosync.WaitGroup
	for i := 0; i < prefetchWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for h := range heightCh {
				blk, err := s.node.GetBlockByHeight(ctx, h)
				resultCh <- fetchResult{height: h, block: blk, err: err}
			}
		}()
	}

	go func() {
		defer func() {
			close(heightCh)
			wg.Wait()
			close(resultCh)
		}()
		for h := from; h <= to; h++ {
			select {
			case heightCh <- h:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Collect results in order using a buffer map
	pending := make(map[int64]*node.Block)
	nextHeight := from

	var batchOutputs []db.TxOutput
	var batchAddrTxs []db.AddressTx
	var batchReviews []db.ProposalReview
	var batchProposals []db.Proposal
	var lastHash string
	batchStart := from
	localCache := make(outputCache)

	for result := range resultCh {
		if result.err != nil {
			return fmt.Errorf("fetch block %d: %w", result.height, result.err)
		}
		pending[result.height] = result.block

		for {
			blk, ok := pending[nextHeight]
			if !ok {
				break
			}
			delete(pending, nextHeight)

			outputs, addrTxs, reviews, err := s.processBlock(ctx, blk, localCache)
			if err != nil {
				return fmt.Errorf("process block %d: %w", nextHeight, err)
			}

			batchOutputs = append(batchOutputs, outputs...)
			batchAddrTxs = append(batchAddrTxs, addrTxs...)
			batchReviews = append(batchReviews, reviews...)

			for _, r := range reviews {
				exists, _ := s.db.ProposalExists(ctx, r.ProposalHash)
				if !exists {
					proposal := s.fetchProposalTitle(ctx, r.ProposalHash, blk.Time)
					if proposal != nil {
						batchProposals = append(batchProposals, *proposal)
					}
				}
			}

			lastHash = blk.Hash
			nextHeight++

			if nextHeight-batchStart >= bulkBatchSize || nextHeight > to {
				if err := s.flushBulkBatch(ctx, batchOutputs, batchAddrTxs, batchReviews, batchProposals, nextHeight-1, lastHash); err != nil {
					return err
				}
				s.height.Store(nextHeight - 1)
				batchOutputs = batchOutputs[:0]
				batchAddrTxs = batchAddrTxs[:0]
				batchReviews = batchReviews[:0]
				batchProposals = batchProposals[:0]
				batchStart = nextHeight
				localCache = make(outputCache)
			}

			if nextHeight%progressInterval == 0 {
				log.Printf("[sync] processed %d / %d (%.1f%%)",
					nextHeight-1, to, float64(nextHeight-1-from)/float64(to-from+1)*100)
			}
		}
	}

	log.Printf("[sync] rebuilding indexes")
	if err := s.db.RebuildIndexes(ctx); err != nil {
		return fmt.Errorf("rebuild indexes: %w", err)
	}
	log.Printf("[sync] rebuilding address_tx_counts")
	if err := s.db.RebuildCounts(ctx); err != nil {
		return fmt.Errorf("rebuild counts: %w", err)
	}
	log.Printf("[sync] running ANALYZE")
	if err := s.db.Analyze(ctx); err != nil {
		return fmt.Errorf("analyze: %w", err)
	}

	return nil
}

func (s *Syncer) flushBulkBatch(ctx context.Context, outputs []db.TxOutput, addrTxs []db.AddressTx, reviews []db.ProposalReview, proposals []db.Proposal, height int64, hash string) error {
	return s.db.BulkInsertBatch(ctx, outputs, addrTxs, reviews, proposals, height, hash)
}

// liveSync polls for new blocks every 2 seconds.
func (s *Syncer) liveSync(ctx context.Context) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.pollOnce(ctx); err != nil {
				log.Printf("[sync] poll error: %v", err)
			}
		}
	}
}

func (s *Syncer) pollOnce(ctx context.Context) error {
	chainHeight, err := s.node.GetBlockCount(ctx)
	if err != nil {
		return err
	}
	chainHeight--

	current := s.height.Load()
	if chainHeight <= current {
		return nil
	}

	for h := current + 1; h <= chainHeight; h++ {
		block, err := s.node.GetBlockByHeight(ctx, h)
		if err != nil {
			return fmt.Errorf("fetch block %d: %w", h, err)
		}

		// Reorg detection: verify parent hash
		if h > 0 {
			lastHash, _ := s.db.GetSyncState(ctx, "last_hash")
			if lastHash != "" && block.PreviousBlockHash != lastHash {
				log.Printf("[sync] reorg detected at %d, rolling back", h)
				if err := s.db.RollbackBlock(ctx, h-1); err != nil {
					return fmt.Errorf("rollback %d: %w", h-1, err)
				}
				newTip := h - 2
				if newTip >= 0 {
					parentBlock, err := s.node.GetBlockByHeight(ctx, newTip)
					if err != nil {
						return fmt.Errorf("fetch parent block %d after reorg: %w", newTip, err)
					}
					if err := s.db.SetSyncState(ctx, "last_hash", parentBlock.Hash); err != nil {
						return fmt.Errorf("update last_hash after reorg: %w", err)
					}
				} else {
					if err := s.db.SetSyncState(ctx, "last_hash", ""); err != nil {
						return fmt.Errorf("reset last_hash after reorg: %w", err)
					}
				}
				if newTip < 0 {
					newTip = 0
				}
				s.height.Store(newTip)
				return nil // will retry on next poll
			}
		}

		blockCache := make(outputCache)
		outputs, addrTxs, reviews, err := s.processBlock(ctx, block, blockCache)
		if err != nil {
			return fmt.Errorf("process block %d: %w", h, err)
		}

		var proposals []db.Proposal
		for _, r := range reviews {
			exists, _ := s.db.ProposalExists(ctx, r.ProposalHash)
			if !exists {
				proposal := s.fetchProposalTitle(ctx, r.ProposalHash, block.Time)
				if proposal != nil {
					proposals = append(proposals, *proposal)
				}
			}
		}

		data := &db.BlockData{
			Height:    h,
			Hash:      block.Hash,
			Outputs:   outputs,
			AddrTxs:   addrTxs,
			Reviews:   reviews,
			Proposals: proposals,
		}
		if err := s.db.InsertBlock(ctx, data); err != nil {
			return fmt.Errorf("insert block %d: %w", h, err)
		}

		s.height.Store(h)
		if s.onNewBlock != nil {
			s.onNewBlock()
		}
		log.Printf("[sync] block %d (%d txs)", h, len(block.Tx))
	}
	return nil
}

type cachedOutput struct {
	Address  string
	ValueSats int64
}

// outputCache holds outputs not yet flushed to DB, keyed by "txid:vout".
type outputCache map[string]cachedOutput

func (c outputCache) add(txid string, n int, addr string, sats int64) {
	c[fmt.Sprintf("%s:%d", txid, n)] = cachedOutput{Address: addr, ValueSats: sats}
}

func (c outputCache) lookup(txid string, n int) (string, int64, bool) {
	o, ok := c[fmt.Sprintf("%s:%d", txid, n)]
	return o.Address, o.ValueSats, ok
}

// processBlock applies the direction algorithm to every transaction.
// The cache allows resolving inputs that reference outputs not yet written to DB
// (intra-block and intra-batch spending).
func (s *Syncer) processBlock(ctx context.Context, block *node.Block, cache outputCache) (
	[]db.TxOutput, []db.AddressTx, []db.ProposalReview, error,
) {
	var allOutputs []db.TxOutput
	var allAddrTxs []db.AddressTx
	var allReviews []db.ProposalReview

	for _, tx := range block.Tx {
		outputs, addrTxs, review, err := s.processTx(ctx, &tx, block.Height, block.Time, cache)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("tx %s: %w", tx.TxID, err)
		}
		for _, o := range outputs {
			cache.add(o.TxID, o.N, o.Address, o.Value)
		}
		allOutputs = append(allOutputs, outputs...)
		allAddrTxs = append(allAddrTxs, addrTxs...)
		if review != nil {
			allReviews = append(allReviews, *review)
		}
	}

	return allOutputs, allAddrTxs, allReviews, nil
}

func (s *Syncer) processTx(ctx context.Context, tx *node.Transaction, height, blockTime int64, cache outputCache) (
	[]db.TxOutput, []db.AddressTx, *db.ProposalReview, error,
) {
	var outputs []db.TxOutput
	outputsByAddr := make(map[string]int64)

	for _, vout := range tx.Vout {
		if vout.Address == "" {
			continue
		}
		sats, err := validate.ELAToSats(vout.Value)
		if err != nil {
			log.Printf("[sync] warning: bad vout value in tx %s vout %d: %v", tx.TxID, vout.N, err)
			continue
		}
		outputs = append(outputs, db.TxOutput{
			TxID:    tx.TxID,
			N:       vout.N,
			Address: vout.Address,
			Value:   sats,
		})
		outputsByAddr[vout.Address] += sats
	}

	memo := tx.ExtractMemo()
	voteCategory := tx.ExtractVoteCategory()

	isCoinbase := len(tx.Vin) > 0 && tx.Vin[0].IsCoinbase()

	var addrTxs []db.AddressTx

	if isCoinbase {
		inputAddrs := []string{"4oLvT2"}
		for addr, sats := range outputsByAddr {
			addrTxs = append(addrTxs, db.AddressTx{
				Address:      addr,
				TxID:         tx.TxID,
				Height:       height,
				Direction:    "received",
				Value:        sats,
				Fee:          0,
				Timestamp:    blockTime,
				TxType:       tx.Type,
				VoteCategory: voteCategory,
				Memo:         memo,
				Inputs:       inputAddrs,
				Outputs:      []string{addr},
			})
		}
	} else {
		inputsByAddr := make(map[string]int64)
		var totalInputSats int64

		for _, vin := range tx.Vin {
			addr, sats, found := cache.lookup(vin.TxID, vin.Vout)
			if !found {
				var err error
				addr, sats, err = s.db.ResolveInput(ctx, vin.TxID, vin.Vout)
				if err != nil {
					log.Printf("[sync] warning: unresolvable input %s:%d in tx %s: %v",
						truncHash(vin.TxID), vin.Vout, truncHash(tx.TxID), err)
					continue
				}
			}
			inputsByAddr[addr] += sats
			totalInputSats += sats
		}

		var totalOutputSats int64
		for _, sats := range outputsByAddr {
			totalOutputSats += sats
		}
		txFee := totalInputSats - totalOutputSats
		if txFee < 0 {
			txFee = 0
		}

		// Build unique address lists
		inputAddrs := uniqueKeys(inputsByAddr)
		outputAddrs := uniqueKeys(outputsByAddr)
		inputAddrSet := toSet(inputAddrs)

		// SENT: each input address gets one "sent" row
		for addr, inputSats := range inputsByAddr {
			changeSats := outputsByAddr[addr] // 0 if no change output
			sentValue := inputSats - changeSats
			if sentValue < 0 {
				sentValue = 0
			}
			addrTxs = append(addrTxs, db.AddressTx{
				Address:      addr,
				TxID:         tx.TxID,
				Height:       height,
				Direction:    "sent",
				Value:        sentValue,
				Fee:          txFee,
				Timestamp:    blockTime,
				TxType:       tx.Type,
				VoteCategory: voteCategory,
				Memo:         memo,
				Inputs:       inputAddrs,
				Outputs:      outputAddrs,
			})
		}

		// RECEIVED: each output address NOT in inputs gets one "received" row
		for addr, sats := range outputsByAddr {
			if inputAddrSet[addr] {
				continue // already counted as "sent" (change goes back)
			}
			addrTxs = append(addrTxs, db.AddressTx{
				Address:      addr,
				TxID:         tx.TxID,
				Height:       height,
				Direction:    "received",
				Value:        sats,
				Fee:          0,
				Timestamp:    blockTime,
				TxType:       tx.Type,
				VoteCategory: voteCategory,
				Memo:         memo,
				Inputs:       inputAddrs,
				Outputs:      []string{addr}, // CRITICAL: only the receiving address
			})
		}
	}

	// 3. Extract CRCProposalReview data (txtype 0x26 = 38)
	var review *db.ProposalReview
	if reviewData := tx.ExtractCRCProposalReview(); reviewData != nil {
		review = &db.ProposalReview{
			DID:            reviewData.DID,
			ProposalHash:   reviewData.ProposalHash,
			Opinion:        reviewData.Opinion,
			OpinionHash:    reviewData.OpinionHash,
			OpinionMessage: reviewData.OpinionMessage,
			ReviewHeight:   height,
			ReviewTime:     blockTime,
		}
	}

	return outputs, addrTxs, review, nil
}

func (s *Syncer) fetchProposalTitle(ctx context.Context, proposalHash string, blockTime int64) *db.Proposal {
	// Try reference endpoint first (has titles), then local node
	clients := []*node.Client{s.refNode, s.node}
	for _, c := range clients {
		if c == nil {
			continue
		}
		state, err := c.GetCRProposalState(ctx, proposalHash)
		if err != nil {
			continue
		}
		return &db.Proposal{
			ProposalHash: proposalHash,
			Title:        state.Title,
			State:        state.Status,
			LastUpdated:  blockTime,
		}
	}
	// If both fail, store with empty title (can be filled by reconciliation)
	return &db.Proposal{
		ProposalHash: proposalHash,
		Title:        "",
		State:        "",
		LastUpdated:  blockTime,
	}
}

func uniqueKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sortStrings(keys)
	return keys
}

func toSet(ss []string) map[string]bool {
	m := make(map[string]bool, len(ss))
	for _, s := range ss {
		m[s] = true
	}
	return m
}

func truncHash(s string) string {
	if len(s) > 16 {
		return s[:16]
	}
	return s
}

func sortStrings(ss []string) {
	for i := 1; i < len(ss); i++ {
		for j := i; j > 0 && strings.Compare(ss[j-1], ss[j]) > 0; j-- {
			ss[j-1], ss[j] = ss[j], ss[j-1]
		}
	}
}
