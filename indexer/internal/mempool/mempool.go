package mempool

import (
	"context"
	"log"
	"sync"
	"time"

	"ela-indexer/internal/db"
	"ela-indexer/internal/node"
	"ela-indexer/internal/validate"
)

const (
	pollInterval    = 3 * time.Second
	maxMempoolSize  = 5000
	txFetchTimeout  = 5 * time.Second
)

// PendingEntry mirrors the historyEntry shape so the RPC layer can prepend it directly.
type PendingEntry struct {
	TxID         string
	Direction    string // "sent" or "received"
	Value        int64  // sats
	Fee          int64  // sats
	TxType       int
	VoteCategory int
	Memo         string
	Inputs       []string
	Outputs      []string
}

// Index maintains an in-memory mapping of address → pending transactions
// by polling the ELA node's mempool. It never writes to the database.
type Index struct {
	node *node.Client
	db   *db.DB

	mu      sync.RWMutex
	byAddr  map[string][]PendingEntry
	known   map[string]struct{} // txids currently in the index
}

func NewIndex(nodeClient *node.Client, database *db.DB) *Index {
	return &Index{
		node:   nodeClient,
		db:     database,
		byAddr: make(map[string][]PendingEntry),
		known:  make(map[string]struct{}),
	}
}

// ForAddress returns pending entries for an address. Safe for concurrent use.
// Returns nil if no pending txs — callers treat nil as "nothing pending."
func (idx *Index) ForAddress(addr string) []PendingEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	entries := idx.byAddr[addr]
	if len(entries) == 0 {
		return nil
	}
	out := make([]PendingEntry, len(entries))
	copy(out, entries)
	return out
}

// Run starts the background polling loop. Blocks until ctx is cancelled.
// Safe to call in a goroutine. Errors are logged, never fatal.
func (idx *Index) Run(ctx context.Context) {
	log.Printf("[mempool] index started, polling every %s", pollInterval)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[mempool] index stopped")
			return
		case <-ticker.C:
			idx.poll(ctx)
		}
	}
}

func (idx *Index) poll(ctx context.Context) {
	txids, err := idx.node.GetRawMempool(ctx)
	if err != nil {
		log.Printf("[mempool] getrawmempool error: %v", err)
		return
	}

	if len(txids) > maxMempoolSize {
		txids = txids[:maxMempoolSize]
	}

	currentSet := make(map[string]struct{}, len(txids))
	for _, id := range txids {
		currentSet[id] = struct{}{}
	}

	// Find new txids we haven't processed yet
	idx.mu.RLock()
	var newTxids []string
	for _, id := range txids {
		if _, exists := idx.known[id]; !exists {
			newTxids = append(newTxids, id)
		}
	}
	idx.mu.RUnlock()

	// Fetch and index new transactions
	newEntries := make(map[string][]PendingEntry)
	newKnown := make(map[string]struct{})
	for _, txid := range newTxids {
		fetchCtx, cancel := context.WithTimeout(ctx, txFetchTimeout)
		tx, err := idx.node.GetRawTransaction(fetchCtx, txid)
		cancel()
		if err != nil {
			short := txid
			if len(short) > 16 {
				short = short[:16]
			}
			log.Printf("[mempool] getrawtransaction %s: %v", short, err)
			continue
		}

		entries := idx.processPendingTx(ctx, tx)
		for addr, e := range entries {
			newEntries[addr] = append(newEntries[addr], e)
		}
		newKnown[txid] = struct{}{}
	}

	// Rebuild the full index: keep existing entries still in mempool + add new ones
	idx.mu.Lock()
	defer idx.mu.Unlock()

	rebuilt := make(map[string][]PendingEntry)
	rebuiltKnown := make(map[string]struct{})

	// Retain entries whose txid is still in the mempool
	for addr, entries := range idx.byAddr {
		for _, e := range entries {
			if _, still := currentSet[e.TxID]; still {
				rebuilt[addr] = append(rebuilt[addr], e)
				rebuiltKnown[e.TxID] = struct{}{}
			}
		}
	}

	// Add newly processed entries
	for addr, entries := range newEntries {
		rebuilt[addr] = append(rebuilt[addr], entries...)
	}
	for txid := range newKnown {
		rebuiltKnown[txid] = struct{}{}
	}

	idx.byAddr = rebuilt
	idx.known = rebuiltKnown
}

// processPendingTx applies the same sent/received direction logic as sync.processTx
// but resolves inputs from the DB (confirmed outputs) since mempool txs spend confirmed UTXOs.
// Returns map[address]PendingEntry.
func (idx *Index) processPendingTx(ctx context.Context, tx *node.Transaction) map[string]PendingEntry {
	result := make(map[string]PendingEntry)

	outputsByAddr := make(map[string]int64)
	outputAddrs := make([]string, 0)
	for _, vout := range tx.Vout {
		if vout.Address == "" {
			continue
		}
		sats, err := validate.ELAToSats(vout.Value)
		if err != nil {
			continue
		}
		if _, seen := outputsByAddr[vout.Address]; !seen {
			outputAddrs = append(outputAddrs, vout.Address)
		}
		outputsByAddr[vout.Address] += sats
	}

	isCoinbase := len(tx.Vin) > 0 && tx.Vin[0].IsCoinbase()

	memo := tx.ExtractMemo()
	voteCategory := tx.ExtractVoteCategory()

	if isCoinbase {
		for addr, sats := range outputsByAddr {
			result[addr] = PendingEntry{
				TxID:         tx.TxID,
				Direction:    "received",
				Value:        sats,
				Fee:          0,
				TxType:       tx.Type,
				VoteCategory: voteCategory,
				Memo:         memo,
				Inputs:       []string{"4oLvT2"},
				Outputs:      []string{addr},
			}
		}
		return result
	}

	inputsByAddr := make(map[string]int64)
	var totalInputSats int64
	inputAddrs := make([]string, 0)

	for _, vin := range tx.Vin {
		addr, sats, err := idx.db.ResolveInput(ctx, vin.TxID, vin.Vout)
		if err != nil {
			continue
		}
		if _, seen := inputsByAddr[addr]; !seen {
			inputAddrs = append(inputAddrs, addr)
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

	inputAddrSet := make(map[string]bool, len(inputAddrs))
	for _, a := range inputAddrs {
		inputAddrSet[a] = true
	}

	// SENT entries
	for addr, inputSats := range inputsByAddr {
		changeSats := outputsByAddr[addr]
		sentValue := inputSats - changeSats
		if sentValue < 0 {
			sentValue = 0
		}
		result[addr] = PendingEntry{
			TxID:         tx.TxID,
			Direction:    "sent",
			Value:        sentValue,
			Fee:          txFee,
			TxType:       tx.Type,
			VoteCategory: voteCategory,
			Memo:         memo,
			Inputs:       inputAddrs,
			Outputs:      outputAddrs,
		}
	}

	// RECEIVED entries
	for addr, sats := range outputsByAddr {
		if inputAddrSet[addr] {
			continue
		}
		result[addr] = PendingEntry{
			TxID:         tx.TxID,
			Direction:    "received",
			Value:        sats,
			Fee:          0,
			TxType:       tx.Type,
			VoteCategory: voteCategory,
			Memo:         memo,
			Inputs:       inputAddrs,
			Outputs:      []string{addr},
		}
	}

	return result
}
