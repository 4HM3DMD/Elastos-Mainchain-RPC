package rpc

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	gosync "sync"
	"time"

	"ela-indexer/internal/db"
	"ela-indexer/internal/mempool"
	"ela-indexer/internal/node"
	eSync "ela-indexer/internal/sync"
	"ela-indexer/internal/validate"
)

const (
	cacheMaxEntries = 2000
	cacheTTL        = 30 * time.Second
	maxBodySize     = 1 * 1024 * 1024 // 1MB request body limit
	readTimeout     = 10 * time.Second
	writeTimeout    = 30 * time.Second
)

type Server struct {
	db      *db.DB
	node    *node.Client
	syncer  *eSync.Syncer
	mempool *mempool.Index
	cache   *lruCache
	mux     *http.ServeMux
}

func NewServer(database *db.DB, nodeClient *node.Client, syncer *eSync.Syncer) *Server {
	s := &Server{
		db:     database,
		node:   nodeClient,
		syncer: syncer,
		cache:  newLRUCache(cacheMaxEntries, cacheTTL),
	}
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("/", s.handleRPC)
	s.mux.HandleFunc("/health", s.handleHealth)
	return s
}

func (s *Server) SetSyncer(syncer *eSync.Syncer) {
	s.syncer = syncer
}

func (s *Server) SetMempool(idx *mempool.Index) {
	s.mempool = idx
}

func (s *Server) InvalidateCache() {
	s.cache.clear()
}

func (s *Server) ListenAndServe(addr string) error {
	srv := &http.Server{
		Addr:         addr,
		Handler:      s.mux,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}
	log.Printf("[rpc] listening on %s", addr)
	return srv.ListenAndServe()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var height int64
	var synced bool
	if s.syncer != nil {
		height = s.syncer.Height()
		synced = s.syncer.IsSynced()
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"height": height,
		"synced": synced,
	})
}

// --- JSON-RPC handling ---

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      interface{}     `json:"id"`
}

type rpcResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result"`
	ID      interface{} `json:"id"`
	Error   *rpcError   `json:"error"`
}

type rpcError struct {
	Code    int         `json:"code"`
	ID      interface{} `json:"id"`
	Message string      `json:"message"`
}

func (s *Server) handleRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodySize))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	// Detect batch vs single
	trimmed := trimLeftSpace(body)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		s.handleBatch(r.Context(), w, body)
		return
	}

	var req rpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, rpcResponse{
			JSONRPC: "2.0",
			Error:   &rpcError{Code: -32700, Message: "Parse error"},
		})
		return
	}

	resp := s.dispatch(r.Context(), &req)
	writeJSON(w, resp)
}

func (s *Server) handleBatch(ctx context.Context, w http.ResponseWriter, body []byte) {
	var reqs []rpcRequest
	if err := json.Unmarshal(body, &reqs); err != nil {
		writeJSON(w, rpcResponse{
			JSONRPC: "2.0",
			Error:   &rpcError{Code: -32700, Message: "Parse error"},
		})
		return
	}
	if len(reqs) == 0 {
		writeJSON(w, rpcResponse{
			JSONRPC: "2.0",
			Error:   &rpcError{Code: -32600, Message: "Empty batch"},
		})
		return
	}
	if len(reqs) > 50 {
		writeJSON(w, rpcResponse{
			JSONRPC: "2.0",
			Error:   &rpcError{Code: -32600, Message: "Batch too large (max 50)"},
		})
		return
	}

	responses := make([]rpcResponse, len(reqs))
	for i, req := range reqs {
		responses[i] = s.dispatch(ctx, &req)
	}
	writeJSON(w, responses)
}

func (s *Server) dispatch(ctx context.Context, req *rpcRequest) rpcResponse {
	if s.syncer != nil && !s.syncer.IsSynced() {
		return errorResp(req.ID, 42003,
			fmt.Sprintf("Indexer syncing, currently at block %d", s.syncer.Height()))
	}

	switch req.Method {
	case "gethistory":
		return s.handleGetHistory(ctx, req)
	case "getcrmember":
		return s.handleGetCRMember(ctx, req)
	default:
		return rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &rpcError{Code: -32601, ID: req.ID, Message: "Method not found"},
		}
	}
}

// --- gethistory ---

func (s *Server) handleGetHistory(ctx context.Context, req *rpcRequest) rpcResponse {
	var params struct {
		Address string `json:"address"`
		Limit   *int   `json:"limit"`
		Skip    *int   `json:"skip"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return errorResp(req.ID, 42002, "Invalid params")
	}

	if err := validate.ValidateAddress(params.Address); err != nil {
		return errorResp(req.ID, 42002,
			fmt.Sprintf("Invalid address: %s", validate.SanitizeLog(params.Address)))
	}

	// Pagination: missing limit → 10, limit < 0 → 10, limit = 0 → ALL, limit > 50 → error
	limit := 10
	if params.Limit != nil {
		if *params.Limit > 50 {
			return errorResp(req.ID, 42002, "limit exceeds maximum of 50")
		}
		if *params.Limit < 0 {
			limit = 10
		} else {
			limit = *params.Limit
		}
	}

	skip := 0
	if params.Skip != nil {
		skip = *params.Skip
		if skip < 0 {
			skip = 0
		}
		if skip > 10000 {
			return errorResp(req.ID, 42002, "skip exceeds maximum of 10000")
		}
	}

	// Check mempool FIRST — if there are pending txs, bypass cache since
	// a stale cached result would hide them for up to 30 seconds.
	var pendingEntries []historyEntry
	if s.mempool != nil {
		for _, p := range s.mempool.ForAddress(params.Address) {
			inputs := p.Inputs
			if inputs == nil {
				inputs = []string{}
			}
			outputs := p.Outputs
			if outputs == nil {
				outputs = []string{}
			}
			pendingEntries = append(pendingEntries, historyEntry{
				Address:      params.Address,
				TxID:         p.TxID,
				Type:         p.Direction,
				Value:        validate.SatsToELA(p.Value),
				Time:         0,
				Height:       0,
				Fee:          validate.SatsToELA(p.Fee),
				Inputs:       inputs,
				Outputs:      outputs,
				TxType:       p.TxType,
				VoteCategory: p.VoteCategory,
				Memo:         p.Memo,
				Status:       "pending",
			})
		}
	}

	// Only use cache when no pending txs exist for this address
	cacheKey := fmt.Sprintf("history:%s:%d:%d", params.Address, limit, skip)
	if len(pendingEntries) == 0 {
		if cached, ok := s.cache.get(cacheKey); ok {
			return rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: cached}
		}
	}

	rows, totalCount, err := s.db.GetHistory(ctx, params.Address, limit, skip)
	if err != nil {
		log.Printf("[rpc] gethistory error: %v", err)
		return errorResp(req.ID, -32603, "Internal error")
	}

	entries := make([]historyEntry, 0, len(pendingEntries)+len(rows))
	entries = append(entries, pendingEntries...)

	for _, r := range rows {
		var inputs, outputs []string
		json.Unmarshal(r.Inputs, &inputs)
		json.Unmarshal(r.Outputs, &outputs)
		if inputs == nil {
			inputs = []string{}
		}
		if outputs == nil {
			outputs = []string{}
		}

		typeStr := "received"
		if r.Direction == "sent" {
			typeStr = "sent"
		}

		entries = append(entries, historyEntry{
			Address:      r.Address,
			TxID:         r.TxID,
			Type:         typeStr,
			Value:        r.Value,
			Time:         r.Timestamp,
			Height:       r.Height,
			Fee:          r.Fee,
			Inputs:       inputs,
			Outputs:      outputs,
			TxType:       r.TxType,
			VoteCategory: r.VoteCategory,
			Memo:         r.Memo,
			Status:       "confirmed",
		})
	}

	result := historyResult{
		History:    entries,
		TotalCount: totalCount + len(pendingEntries),
	}

	// Don't cache results that include pending txs — they change every few seconds
	if len(pendingEntries) == 0 {
		s.cache.set(cacheKey, result)
	}

	return rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: result}
}

type historyEntry struct {
	Address      string   `json:"address"`
	TxID         string   `json:"txid"`
	Type         string   `json:"type"`
	Value        string   `json:"value"`
	Time         int64    `json:"time"`
	Height       int64    `json:"height"`
	Fee          string   `json:"fee"`
	Inputs       []string `json:"inputs"`
	Outputs      []string `json:"outputs"`
	TxType       int      `json:"txtype"`
	VoteCategory int      `json:"votecategory"`
	Memo         string   `json:"memo"`
	Status       string   `json:"Status"`
}

type historyResult struct {
	History    []historyEntry `json:"txhistory"`
	TotalCount int            `json:"totalcount"`
}

// --- getcrmember ---

func (s *Server) handleGetCRMember(ctx context.Context, req *rpcRequest) rpcResponse {
	var params struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return errorResp(req.ID, 42002, "Invalid params")
	}
	if params.ID == "" {
		return errorResp(req.ID, 42002, "id is required")
	}

	cacheKey := "crmember:" + params.ID
	if cached, ok := s.cache.get(cacheKey); ok {
		return rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: cached}
	}

	// Get live CR member info from node
	members, err := s.node.ListCurrentCRs(ctx)
	if err != nil {
		log.Printf("[rpc] listcurrentcrs error: %v", err)
		return errorResp(req.ID, -32603, "Internal error")
	}

	// Match against both DID and CID (observation 25)
	var member *node.CRMember
	for i := range members {
		if members[i].DID == params.ID || members[i].CID == params.ID {
			member = &members[i]
			break
		}
	}
	if member == nil {
		return errorResp(req.ID, 42002, "CR member not found")
	}

	// Get review/performance data from DB using the resolved DID
	reviews, err := s.db.GetCRMemberReviews(ctx, member.DID)
	if err != nil {
		log.Printf("[rpc] reviews error: %v", err)
		return errorResp(req.ID, -32603, "Internal error")
	}

	performance := make([]performanceEntry, 0, len(reviews))
	for _, r := range reviews {
		performance = append(performance, performanceEntry{
			Title:           r.Title,
			ProposalHash:    r.ProposalHash,
			ProposalState:   r.ProposalState,
			Opinion:         r.Opinion,
			OpinionHash:     r.OpinionHash,
			OpinionMessage:  r.OpinionMessage,
			ReviewHeight:    r.ReviewHeight,
			ReviewTimestamp: r.ReviewTimestamp,
		})
	}

	result := crMemberResult{
		DID:                     member.DID,
		CID:                     member.CID,
		Code:                    member.Code,
		NickName:                member.NickName,
		URL:                     member.URL,
		Location:                member.Location,
		DepositAddress:          member.DepositAddress,
		DepositAmount:           member.DepositAmount,
		DPoSPublicKey:           member.DPOSPublicKey,
		ImpeachmentVotes:        member.ImpeachmentVotes,
		ImpeachmentThroughVotes: "",
		Penalty:                 member.Penalty,
		Term:                    []int{},
		Performance:             performance,
		State:                   member.State,
	}

	s.cache.set(cacheKey, result)

	return rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: result}
}

type crMemberResult struct {
	DID                     string             `json:"did"`
	CID                     string             `json:"cid"`
	Code                    string             `json:"code"`
	NickName                string             `json:"nickname"`
	URL                     string             `json:"url"`
	Location                int                `json:"location"`
	DepositAddress          string             `json:"depositaddress"`
	DepositAmount           string             `json:"depositamout"` // intentional typo match
	DPoSPublicKey           string             `json:"dpospublickey"`
	ImpeachmentVotes        string             `json:"impeachmentvotes"`
	ImpeachmentThroughVotes string             `json:"impeachmentThroughVotes"`
	Penalty                 string             `json:"penalty"`
	Term                    []int              `json:"term"`
	Performance             []performanceEntry `json:"performance"`
	State                   string             `json:"state"`
}

type performanceEntry struct {
	Title           string `json:"title"`
	ProposalHash    string `json:"proposalHash"`
	ProposalState   string `json:"proposalState"`
	Opinion         string `json:"opinion"`
	OpinionHash     string `json:"opinionHash"`
	OpinionMessage  string `json:"opinionMessage"`
	ReviewHeight    int64  `json:"reviewHeight"`
	ReviewTimestamp int64  `json:"reviewTimestamp"`
}

// --- helpers ---

func errorResp(id interface{}, code int, msg string) rpcResponse {
	return rpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &rpcError{
			Code:    code,
			ID:      id,
			Message: msg,
		},
	}
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func trimLeftSpace(b []byte) []byte {
	for i, c := range b {
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			return b[i:]
		}
	}
	return nil
}

// --- LRU cache with TTL ---

type cacheEntry struct {
	key       string
	value     interface{}
	expiresAt time.Time
}

type lruCache struct {
	mu       gosync.Mutex
	capacity int
	ttl      time.Duration
	items    map[string]*list.Element
	order    *list.List
}

func newLRUCache(capacity int, ttl time.Duration) *lruCache {
	return &lruCache{
		capacity: capacity,
		ttl:      ttl,
		items:    make(map[string]*list.Element),
		order:    list.New(),
	}
}

func (c *lruCache) get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	entry := el.Value.(*cacheEntry)
	if time.Now().After(entry.expiresAt) {
		c.order.Remove(el)
		delete(c.items, key)
		return nil, false
	}
	c.order.MoveToFront(el)
	return entry.value, true
}

func (c *lruCache) set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		c.order.MoveToFront(el)
		entry := el.Value.(*cacheEntry)
		entry.value = value
		entry.expiresAt = time.Now().Add(c.ttl)
		return
	}

	if c.order.Len() >= c.capacity {
		oldest := c.order.Back()
		if oldest != nil {
			c.order.Remove(oldest)
			delete(c.items, oldest.Value.(*cacheEntry).key)
		}
	}

	entry := &cacheEntry{
		key:       key,
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}
	el := c.order.PushFront(entry)
	c.items[key] = el
}

func (c *lruCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]*list.Element)
	c.order.Init()
}
