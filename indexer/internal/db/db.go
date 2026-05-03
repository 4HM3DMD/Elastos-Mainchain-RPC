package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"ela-indexer/internal/validate"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return &DB{pool: pool}, nil
}

func (d *DB) Close() {
	d.pool.Close()
}

// --- sync_state ---

func (d *DB) GetSyncState(ctx context.Context, key string) (string, error) {
	var val string
	err := d.pool.QueryRow(ctx,
		"SELECT value FROM sync_state WHERE key = $1", key,
	).Scan(&val)
	if err == pgx.ErrNoRows {
		return "", nil
	}
	return val, err
}

func (d *DB) SetSyncState(ctx context.Context, key, value string) error {
	_, err := d.pool.Exec(ctx,
		`INSERT INTO sync_state (key, value) VALUES ($1, $2)
		 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		key, value,
	)
	return err
}

// --- tx_outputs (for resolving inputs) ---

func (d *DB) ResolveInput(ctx context.Context, txid string, n int) (address string, valueSats int64, err error) {
	var valueStr string
	err = d.pool.QueryRow(ctx,
		"SELECT address, value FROM tx_outputs WHERE txid = $1 AND n = $2",
		txid, n,
	).Scan(&address, &valueStr)
	if err != nil {
		return "", 0, err
	}
	valueSats, err = strconv.ParseInt(valueStr, 10, 64)
	return
}

// --- Block insertion (atomic) ---

type BlockData struct {
	Height    int64
	Hash      string
	Outputs   []TxOutput
	AddrTxs   []AddressTx
	Reviews   []ProposalReview
	Proposals []Proposal
}

type TxOutput struct {
	TxID    string
	N       int
	Address string
	Value   int64 // satoshis
}

type AddressTx struct {
	Address      string
	TxID         string
	Height       int64
	Direction    string // "sent" or "received"
	Value        int64  // satoshis
	Fee          int64  // satoshis
	Timestamp    int64
	TxType       int
	VoteCategory int
	Memo         string
	Inputs       []string
	Outputs      []string
}

type ProposalReview struct {
	DID            string
	ProposalHash   string
	Opinion        string
	OpinionHash    string
	OpinionMessage string
	ReviewHeight   int64
	ReviewTime     int64
}

type Proposal struct {
	ProposalHash string
	Title        string
	State        string
	LastUpdated  int64
}

func (d *DB) InsertBlock(ctx context.Context, data *BlockData) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Insert tx_outputs
	for _, o := range data.Outputs {
		_, err := tx.Exec(ctx,
			`INSERT INTO tx_outputs (txid, n, address, value)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT DO NOTHING`,
			o.TxID, o.N, o.Address, strconv.FormatInt(o.Value, 10),
		)
		if err != nil {
			return fmt.Errorf("insert tx_output: %w", err)
		}
	}

	// 2. Insert address_transactions and update counts
	addrDelta := make(map[string]int)
	for _, at := range data.AddrTxs {
		inputsJSON, _ := json.Marshal(at.Inputs)
		outputsJSON, _ := json.Marshal(at.Outputs)
		_, err := tx.Exec(ctx,
			`INSERT INTO address_transactions
			 (address, txid, height, direction, value, fee, timestamp, tx_type, vote_category, memo, inputs, outputs)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			 ON CONFLICT DO NOTHING`,
			at.Address, at.TxID, at.Height, at.Direction,
			validate.SatsToELA(at.Value),
			validate.SatsToELA(at.Fee),
			at.Timestamp, at.TxType, at.VoteCategory, at.Memo,
			string(inputsJSON), string(outputsJSON),
		)
		if err != nil {
			return fmt.Errorf("insert address_tx: %w", err)
		}
		addrDelta[at.Address]++
	}

	// 3. Update address_tx_counts
	for addr, delta := range addrDelta {
		_, err := tx.Exec(ctx,
			`INSERT INTO address_tx_counts (address, tx_count)
			 VALUES ($1, $2)
			 ON CONFLICT (address) DO UPDATE SET tx_count = address_tx_counts.tx_count + EXCLUDED.tx_count`,
			addr, delta,
		)
		if err != nil {
			return fmt.Errorf("update addr count: %w", err)
		}
	}

	// 4. Insert cr_proposal_reviews
	for _, r := range data.Reviews {
		_, err := tx.Exec(ctx,
			`INSERT INTO cr_proposal_reviews
			 (did, proposal_hash, opinion, opinion_hash, opinion_message, review_height, review_timestamp)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)
			 ON CONFLICT DO NOTHING`,
			r.DID, r.ProposalHash, r.Opinion, r.OpinionHash,
			r.OpinionMessage, r.ReviewHeight, r.ReviewTime,
		)
		if err != nil {
			return fmt.Errorf("insert review: %w", err)
		}
	}

	// 5. Upsert cr_proposals
	for _, p := range data.Proposals {
		_, err := tx.Exec(ctx,
			`INSERT INTO cr_proposals (proposal_hash, title, state, last_updated)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (proposal_hash) DO UPDATE
			 SET title = CASE WHEN EXCLUDED.title != '' THEN EXCLUDED.title ELSE cr_proposals.title END,
			     state = CASE WHEN EXCLUDED.state != '' THEN EXCLUDED.state ELSE cr_proposals.state END,
			     last_updated = EXCLUDED.last_updated`,
			p.ProposalHash, p.Title, p.State, p.LastUpdated,
		)
		if err != nil {
			return fmt.Errorf("upsert proposal: %w", err)
		}
	}

	// 6. Update sync_state
	_, err = tx.Exec(ctx,
		`INSERT INTO sync_state (key, value) VALUES ('last_height', $1)
		 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		strconv.FormatInt(data.Height, 10),
	)
	if err != nil {
		return fmt.Errorf("update last_height: %w", err)
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO sync_state (key, value) VALUES ('last_hash', $1)
		 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		data.Hash,
	)
	if err != nil {
		return fmt.Errorf("update last_hash: %w", err)
	}

	return tx.Commit(ctx)
}

// BulkInsertBatch atomically inserts a batch of block data during initial sync.
// All COPYs and sync_state updates happen in a single transaction for crash safety.
func (d *DB) BulkInsertBatch(ctx context.Context, outputs []TxOutput, addrTxs []AddressTx,
	reviews []ProposalReview, proposals []Proposal, height int64, hash string) error {

	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin bulk tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// COPY tx_outputs
	if len(outputs) > 0 {
		outRows := make([][]interface{}, len(outputs))
		for i, o := range outputs {
			outRows[i] = []interface{}{o.TxID, o.N, o.Address, strconv.FormatInt(o.Value, 10)}
		}
		_, err = tx.CopyFrom(ctx,
			pgx.Identifier{"tx_outputs"},
			[]string{"txid", "n", "address", "value"},
			pgx.CopyFromRows(outRows),
		)
		if err != nil {
			return fmt.Errorf("copy tx_outputs: %w", err)
		}
	}

	// COPY address_transactions
	if len(addrTxs) > 0 {
		atRows := make([][]interface{}, len(addrTxs))
		for i, at := range addrTxs {
			inputsJSON, _ := json.Marshal(at.Inputs)
			outputsJSON, _ := json.Marshal(at.Outputs)
			atRows[i] = []interface{}{
				at.Address, at.TxID, at.Height, at.Direction,
				validate.SatsToELA(at.Value),
				validate.SatsToELA(at.Fee),
				at.Timestamp, at.TxType, at.VoteCategory, at.Memo,
				string(inputsJSON), string(outputsJSON),
			}
		}
		_, err = tx.CopyFrom(ctx,
			pgx.Identifier{"address_transactions"},
			[]string{"address", "txid", "height", "direction", "value", "fee",
				"timestamp", "tx_type", "vote_category", "memo", "inputs", "outputs"},
			pgx.CopyFromRows(atRows),
		)
		if err != nil {
			return fmt.Errorf("copy address_transactions: %w", err)
		}
	}

	// Upsert cr_proposal_reviews (can't COPY due to duplicate key from re-reviews)
	for _, r := range reviews {
		_, err := tx.Exec(ctx,
			`INSERT INTO cr_proposal_reviews (did, proposal_hash, opinion, opinion_hash,
				opinion_message, review_height, review_timestamp)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)
			 ON CONFLICT (did, proposal_hash) DO UPDATE
			 SET opinion = EXCLUDED.opinion,
			     opinion_hash = EXCLUDED.opinion_hash,
			     opinion_message = EXCLUDED.opinion_message,
			     review_height = EXCLUDED.review_height,
			     review_timestamp = EXCLUDED.review_timestamp`,
			r.DID, r.ProposalHash, r.Opinion, r.OpinionHash,
			r.OpinionMessage, r.ReviewHeight, r.ReviewTime,
		)
		if err != nil {
			return fmt.Errorf("upsert cr_proposal_review: %w", err)
		}
	}

	// Upsert cr_proposals
	for _, p := range proposals {
		_, err := tx.Exec(ctx,
			`INSERT INTO cr_proposals (proposal_hash, title, state, last_updated)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (proposal_hash) DO UPDATE
			 SET title = CASE WHEN EXCLUDED.title != '' THEN EXCLUDED.title ELSE cr_proposals.title END,
			     state = CASE WHEN EXCLUDED.state != '' THEN EXCLUDED.state ELSE cr_proposals.state END,
			     last_updated = EXCLUDED.last_updated`,
			p.ProposalHash, p.Title, p.State, p.LastUpdated,
		)
		if err != nil {
			return fmt.Errorf("upsert proposal: %w", err)
		}
	}

	// Update sync_state
	for _, pair := range [][2]string{
		{"last_height", strconv.FormatInt(height, 10)},
		{"last_hash", hash},
	} {
		_, err = tx.Exec(ctx,
			`INSERT INTO sync_state (key, value) VALUES ($1, $2)
			 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
			pair[0], pair[1],
		)
		if err != nil {
			return fmt.Errorf("update sync_state %s: %w", pair[0], err)
		}
	}

	return tx.Commit(ctx)
}

// --- Block rollback (reorg) ---

func (d *DB) RollbackBlock(ctx context.Context, height int64) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin rollback tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Count address_transactions being removed per address
	rows, err := tx.Query(ctx,
		"SELECT address, COUNT(*) FROM address_transactions WHERE height = $1 GROUP BY address",
		height,
	)
	if err != nil {
		return fmt.Errorf("count rollback addrs: %w", err)
	}
	addrCounts := make(map[string]int)
	for rows.Next() {
		var addr string
		var count int
		if err := rows.Scan(&addr, &count); err != nil {
			rows.Close()
			return err
		}
		addrCounts[addr] = count
	}
	rows.Close()

	// Delete address_transactions at this height
	_, err = tx.Exec(ctx,
		"DELETE FROM address_transactions WHERE height = $1", height,
	)
	if err != nil {
		return fmt.Errorf("delete addr_txs: %w", err)
	}

	// Decrement counts
	for addr, count := range addrCounts {
		_, err := tx.Exec(ctx,
			"UPDATE address_tx_counts SET tx_count = tx_count - $1 WHERE address = $2",
			count, addr,
		)
		if err != nil {
			return fmt.Errorf("decrement count: %w", err)
		}
	}

	// Delete tx_outputs from transactions in this block
	// We find txids from the address_transactions we just deleted... but we already deleted them.
	// Instead, find txids from tx_outputs that were created at this height.
	// Since tx_outputs doesn't have height, we use the address_transactions backup.
	// Actually we need to be smarter: we stored outputs for all txs in the block.
	// For now, we don't delete tx_outputs during rollback — they're idempotent
	// and re-processing will re-insert them with ON CONFLICT DO NOTHING.

	// Delete cr_proposal_reviews at this height
	_, err = tx.Exec(ctx,
		"DELETE FROM cr_proposal_reviews WHERE review_height = $1", height,
	)
	if err != nil {
		return fmt.Errorf("delete reviews: %w", err)
	}

	// Rewind sync_state
	prevHeight := height - 1
	_, err = tx.Exec(ctx,
		`UPDATE sync_state SET value = $1 WHERE key = 'last_height'`,
		strconv.FormatInt(prevHeight, 10),
	)
	if err != nil {
		return fmt.Errorf("rewind height: %w", err)
	}

	return tx.Commit(ctx)
}

// --- Query: gethistory ---

type HistoryRow struct {
	Address      string
	TxID         string
	Direction    string
	Value        string
	Timestamp    int64
	Height       int64
	Fee          string
	Inputs       json.RawMessage
	Outputs      json.RawMessage
	TxType       int
	VoteCategory int
	Memo         string
}

func (d *DB) GetHistory(ctx context.Context, address string, limit, skip int) ([]HistoryRow, int, error) {
	// Get total count from dedicated count table
	var totalCount int
	err := d.pool.QueryRow(ctx,
		"SELECT COALESCE(tx_count, 0) FROM address_tx_counts WHERE address = $1",
		address,
	).Scan(&totalCount)
	if err == pgx.ErrNoRows {
		totalCount = 0
	} else if err != nil {
		return nil, 0, fmt.Errorf("count query: %w", err)
	}

	if totalCount == 0 || skip >= totalCount {
		return nil, totalCount, nil
	}

	var query string
	var args []interface{}

	if limit == 0 {
		// limit=0 means ALL, but cap at 5000 for safety
		query = `SELECT address, txid, direction, value, fee, timestamp, height,
				        tx_type, vote_category, memo, inputs, outputs
				 FROM address_transactions
				 WHERE address = $1
				 ORDER BY height DESC
				 OFFSET $2 LIMIT 5000`
		args = []interface{}{address, skip}
	} else {
		query = `SELECT address, txid, direction, value, fee, timestamp, height,
				        tx_type, vote_category, memo, inputs, outputs
				 FROM address_transactions
				 WHERE address = $1
				 ORDER BY height DESC
				 OFFSET $2 LIMIT $3`
		args = []interface{}{address, skip, limit}
	}

	rows, err := d.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("history query: %w", err)
	}
	defer rows.Close()

	var results []HistoryRow
	for rows.Next() {
		var r HistoryRow
		if err := rows.Scan(
			&r.Address, &r.TxID, &r.Direction, &r.Value, &r.Fee,
			&r.Timestamp, &r.Height, &r.TxType, &r.VoteCategory,
			&r.Memo, &r.Inputs, &r.Outputs,
		); err != nil {
			return nil, 0, fmt.Errorf("scan history: %w", err)
		}
		results = append(results, r)
	}
	return results, totalCount, nil
}

// --- Query: getcrmember review data ---

type ReviewRow struct {
	ProposalHash    string
	Title           string
	ProposalState   string
	Opinion         string
	OpinionHash     string
	OpinionMessage  string
	ReviewHeight    int64
	ReviewTimestamp int64
}

func (d *DB) GetCRMemberReviews(ctx context.Context, did string) ([]ReviewRow, error) {
	rows, err := d.pool.Query(ctx,
		`SELECT r.proposal_hash,
		        COALESCE(p.title, '') AS title,
		        COALESCE(p.state, '') AS proposal_state,
		        r.opinion, r.opinion_hash, r.opinion_message,
		        r.review_height, r.review_timestamp
		 FROM cr_proposal_reviews r
		 LEFT JOIN cr_proposals p ON r.proposal_hash = p.proposal_hash
		 WHERE r.did = $1
		 ORDER BY r.review_height DESC`,
		did,
	)
	if err != nil {
		return nil, fmt.Errorf("reviews query: %w", err)
	}
	defer rows.Close()

	var results []ReviewRow
	for rows.Next() {
		var r ReviewRow
		if err := rows.Scan(
			&r.ProposalHash, &r.Title, &r.ProposalState,
			&r.Opinion, &r.OpinionHash, &r.OpinionMessage,
			&r.ReviewHeight, &r.ReviewTimestamp,
		); err != nil {
			return nil, fmt.Errorf("scan review: %w", err)
		}
		results = append(results, r)
	}
	return results, nil
}

// --- Index management for initial sync ---

func (d *DB) DropSecondaryIndexes(ctx context.Context) error {
	indexes := []string{
		"DROP INDEX IF EXISTS idx_addrtx_addr_height",
		"DROP INDEX IF EXISTS idx_addrtx_height",
	}
	for _, q := range indexes {
		if _, err := d.pool.Exec(ctx, q); err != nil {
			return fmt.Errorf("drop index: %w", err)
		}
	}
	return nil
}

func (d *DB) RebuildIndexes(ctx context.Context) error {
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_addrtx_addr_height ON address_transactions (address, height DESC)",
		"CREATE INDEX IF NOT EXISTS idx_addrtx_height ON address_transactions (height)",
	}
	for _, q := range indexes {
		if _, err := d.pool.Exec(ctx, q); err != nil {
			return fmt.Errorf("rebuild index: %w", err)
		}
	}
	return nil
}

func (d *DB) Analyze(ctx context.Context) error {
	tables := []string{"tx_outputs", "address_transactions", "address_tx_counts",
		"cr_proposal_reviews", "cr_proposals"}
	for _, t := range tables {
		if _, err := d.pool.Exec(ctx, "ANALYZE "+t); err != nil {
			return fmt.Errorf("analyze %s: %w", t, err)
		}
	}
	return nil
}

// RebuildCounts rebuilds address_tx_counts from address_transactions after initial sync.
func (d *DB) RebuildCounts(ctx context.Context) error {
	_, err := d.pool.Exec(ctx, "TRUNCATE address_tx_counts")
	if err != nil {
		return fmt.Errorf("truncate counts: %w", err)
	}
	_, err = d.pool.Exec(ctx,
		`INSERT INTO address_tx_counts (address, tx_count)
		 SELECT address, COUNT(*) FROM address_transactions GROUP BY address`)
	return err
}

// ProposalExists checks if a proposal_hash already exists in cr_proposals.
func (d *DB) ProposalExists(ctx context.Context, hash string) (bool, error) {
	var exists bool
	err := d.pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM cr_proposals WHERE proposal_hash = $1)", hash,
	).Scan(&exists)
	return exists, err
}

// InsertProposal inserts a single proposal (used during sync when fetching from reference).
func (d *DB) InsertProposal(ctx context.Context, p Proposal) error {
	_, err := d.pool.Exec(ctx,
		`INSERT INTO cr_proposals (proposal_hash, title, state, last_updated)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (proposal_hash) DO UPDATE
		 SET title = CASE WHEN EXCLUDED.title != '' THEN EXCLUDED.title ELSE cr_proposals.title END,
		     state = CASE WHEN EXCLUDED.state != '' THEN EXCLUDED.state ELSE cr_proposals.state END,
		     last_updated = EXCLUDED.last_updated`,
		p.ProposalHash, p.Title, p.State, p.LastUpdated,
	)
	return err
}

// UpdateSyncStateTx updates sync_state within an existing transaction.
func (d *DB) SetSyncStateBatch(ctx context.Context, pairs map[string]string) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for k, v := range pairs {
		_, err := tx.Exec(ctx,
			`INSERT INTO sync_state (key, value) VALUES ($1, $2)
			 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`, k, v)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// SanityCheck validates that sync_state is consistent with actual data.
// Catches the case where data tables were truncated but sync_state was not reset.
func (d *DB) SanityCheck(ctx context.Context) error {
	// Clean up legacy 'height' key that should never exist (only 'last_height' is valid)
	d.pool.Exec(ctx, "DELETE FROM sync_state WHERE key = 'height'")

	heightStr, err := d.GetSyncState(ctx, "last_height")
	if err != nil {
		return fmt.Errorf("sanity check: read last_height: %w", err)
	}
	if heightStr == "" || heightStr == "0" {
		return nil
	}

	height, _ := strconv.ParseInt(heightStr, 10, 64)
	if height < 100 {
		return nil
	}

	var outputCount int64
	err = d.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM tx_outputs LIMIT 1",
	).Scan(&outputCount)
	if err != nil {
		return fmt.Errorf("sanity check: count tx_outputs: %w", err)
	}

	if outputCount == 0 {
		return fmt.Errorf(
			"FATAL: sync_state says last_height=%d but tx_outputs is empty. "+
				"Data was truncated without resetting sync_state. "+
				"Fix: UPDATE sync_state SET value='0' WHERE key='last_height'; "+
				"UPDATE sync_state SET value='' WHERE key='last_hash';",
			height,
		)
	}
	return nil
}
