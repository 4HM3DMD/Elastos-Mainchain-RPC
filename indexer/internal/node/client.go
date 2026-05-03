package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

type Client struct {
	url        string
	username   string
	password   string
	httpClient *http.Client
}

func NewClient(rawURL string) *Client {
	c := &Client{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
	parsed, err := url.Parse(rawURL)
	if err == nil && parsed.User != nil {
		c.username = parsed.User.Username()
		c.password, _ = parsed.User.Password()
		parsed.User = nil
		c.url = parsed.String()
	} else {
		c.url = rawURL
	}
	return c
}

// NewExternalClient creates a client for external endpoints (api.elastos.io) with no auth.
func NewExternalClient(rawURL string) *Client {
	return &Client{
		url: rawURL,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

type rpcRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      int         `json:"id"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (c *Client) call(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: 1, Method: method, Params: params})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("rpc call %s: %w", method, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var rr rpcResponse
	if err := json.Unmarshal(respBody, &rr); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	if rr.Error != nil {
		return nil, fmt.Errorf("rpc error %d: %s", rr.Error.Code, rr.Error.Message)
	}
	return rr.Result, nil
}

func (c *Client) GetRawMempool(ctx context.Context) ([]string, error) {
	raw, err := c.call(ctx, "getrawmempool", nil)
	if err != nil {
		return nil, err
	}
	var txids []string
	if err := json.Unmarshal(raw, &txids); err != nil {
		return nil, fmt.Errorf("parse mempool: %w", err)
	}
	return txids, nil
}

func (c *Client) GetRawTransaction(ctx context.Context, txid string) (*Transaction, error) {
	raw, err := c.call(ctx, "getrawtransaction", map[string]interface{}{
		"txid":    txid,
		"verbose": true,
	})
	if err != nil {
		return nil, err
	}
	var tx Transaction
	if err := json.Unmarshal(raw, &tx); err != nil {
		return nil, fmt.Errorf("parse transaction: %w", err)
	}
	return &tx, nil
}

func (c *Client) GetBlockCount(ctx context.Context) (int64, error) {
	raw, err := c.call(ctx, "getblockcount", nil)
	if err != nil {
		return 0, err
	}
	var count int64
	if err := json.Unmarshal(raw, &count); err != nil {
		return 0, fmt.Errorf("parse blockcount: %w", err)
	}
	return count, nil
}

func (c *Client) GetBlockByHeight(ctx context.Context, height int64) (*Block, error) {
	raw, err := c.call(ctx, "getblockbyheight", map[string]interface{}{
		"height": height,
	})
	if err != nil {
		return nil, err
	}
	var block Block
	if err := json.Unmarshal(raw, &block); err != nil {
		return nil, fmt.Errorf("parse block at %d: %w", height, err)
	}
	return &block, nil
}

func (c *Client) ListCurrentCRs(ctx context.Context) ([]CRMember, error) {
	raw, err := c.call(ctx, "listcurrentcrs", map[string]interface{}{
		"state": "all",
	})
	if err != nil {
		return nil, err
	}
	var result struct {
		CRMemberInfoSlice []CRMember `json:"crmembersinfo"`
		TotalCounts       int        `json:"totalcounts"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("parse listcurrentcrs: %w", err)
	}
	return result.CRMemberInfoSlice, nil
}

func (c *Client) GetCRProposalState(ctx context.Context, proposalHash string) (*ProposalState, error) {
	raw, err := c.call(ctx, "getcrproposalstate", map[string]interface{}{
		"proposalhash": proposalHash,
	})
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		State ProposalState `json:"proposalstate"`
	}
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		return nil, fmt.Errorf("parse proposalstate: %w", err)
	}
	return &wrapper.State, nil
}

// Block represents a full block with transactions from getblockbyheight.
type Block struct {
	Hash              string        `json:"hash"`
	Height            int64         `json:"height"`
	Time              int64         `json:"time"`
	PreviousBlockHash string        `json:"previousblockhash"`
	Tx                []Transaction `json:"tx"`
}

type Transaction struct {
	TxID       string      `json:"txid"`
	Type       int         `json:"type"`
	Payload    interface{} `json:"payload"`
	Attributes []Attribute `json:"attributes"`
	Vin        []Vin       `json:"vin"`
	Vout       []Vout      `json:"vout"`
}

type Vin struct {
	TxID     string `json:"txid"`
	Vout     int    `json:"vout"`
	Sequence int64  `json:"sequence"`
}

type Vout struct {
	Value      string      `json:"value"`
	N          int         `json:"n"`
	Address    string      `json:"address"`
	OutputLock int         `json:"outputlock"`
	Type       int         `json:"type"`
	Payload    interface{} `json:"payload"`
}

type Attribute struct {
	Usage int    `json:"usage"`
	Data  string `json:"data"`
}

type CRMember struct {
	Code             string `json:"code"`
	CID              string `json:"cid"`
	DID              string `json:"did"`
	DPOSPublicKey    string `json:"dpospublickey"`
	NickName         string `json:"nickname"`
	URL              string `json:"url"`
	Location         int    `json:"location"`
	ImpeachmentVotes string `json:"impeachmentvotes"`
	DepositAmount    string `json:"depositamout"`
	DepositAddress   string `json:"depositaddress"`
	Penalty          string `json:"penalty"`
	State            string `json:"state"`
	Index            int    `json:"index"`
}

type ProposalState struct {
	Title              string            `json:"title"`
	Status             string            `json:"status"`
	ProposalHash       string            `json:"proposalhash"`
	CRVotes            map[string]string `json:"crvotes"`
	CROpinions         []CROpinion       `json:"crOpinions"`
	RegisterHeight     int64             `json:"registerheight"`
	RegisterTimestamp  int64             `json:"registerTimestamp"`
}

type CROpinion struct {
	DID     string `json:"did"`
	Hash    string `json:"hash"`
	Message string `json:"message"`
}

// IsCoinbase returns true if this vin is a coinbase input (all-zero txid, vout=65535).
func (v *Vin) IsCoinbase() bool {
	if v.Vout != 65535 {
		return false
	}
	for _, c := range v.TxID {
		if c != '0' {
			return false
		}
	}
	return true
}

// ExtractMemo finds the memo attribute (usage=0x81=129) and returns decoded text.
func (tx *Transaction) ExtractMemo() string {
	for _, attr := range tx.Attributes {
		if attr.Usage == 129 {
			bytes, err := hexDecode(attr.Data)
			if err != nil {
				return ""
			}
			return toValidUTF8(string(bytes))
		}
	}
	return ""
}

// ExtractVoteCategory extracts the vote category from output payloads.
func (tx *Transaction) ExtractVoteCategory() int {
	for _, v := range tx.Vout {
		if v.Type > 0 && v.Payload != nil {
			pm, ok := v.Payload.(map[string]interface{})
			if !ok {
				continue
			}
			if contents, ok := pm["contents"]; ok {
				arr, ok := contents.([]interface{})
				if ok && len(arr) > 0 {
					first, ok := arr[0].(map[string]interface{})
					if ok {
						if vt, ok := first["votetype"]; ok {
							if vtf, ok := vt.(float64); ok {
								return int(vtf)
							}
						}
					}
				}
			}
			if version, ok := pm["version"]; ok {
				if vf, ok := version.(float64); ok && vf > 0 {
					return int(vf)
				}
			}
		}
	}
	return 0
}

// ExtractCRCProposalReview extracts review data from a txtype 38 (CRCProposalReview) payload.
func (tx *Transaction) ExtractCRCProposalReview() *CRCReviewData {
	if tx.Type != 0x26 {
		return nil
	}
	pm, ok := tx.Payload.(map[string]interface{})
	if !ok {
		return nil
	}
	data := &CRCReviewData{}
	if v, ok := pm["proposalhash"].(string); ok {
		data.ProposalHash = v
	}
	if v, ok := pm["voteresult"].(string); ok {
		data.Opinion = v
	}
	if v, ok := pm["opinionhash"].(string); ok {
		data.OpinionHash = v
	}
	if v, ok := pm["opiniondata"].(string); ok {
		data.OpinionMessage = v
	}
	if v, ok := pm["did"].(string); ok {
		data.DID = v
	}
	if data.ProposalHash == "" || data.DID == "" {
		return nil
	}
	return data
}

type CRCReviewData struct {
	ProposalHash  string
	Opinion       string
	OpinionHash   string
	OpinionMessage string
	DID           string
}

func hexDecode(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd length")
	}
	out := make([]byte, len(s)/2)
	for i := 0; i < len(s); i += 2 {
		hi := hexVal(s[i])
		lo := hexVal(s[i+1])
		if hi < 0 || lo < 0 {
			return nil, fmt.Errorf("bad hex")
		}
		out[i/2] = byte(hi<<4 | lo)
	}
	return out, nil
}

func hexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}

func toValidUTF8(s string) string {
	s = strings.ToValidUTF8(s, "\ufffd")
	if strings.IndexByte(s, 0) < 0 {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r != 0 {
			b.WriteRune(r)
		}
		i += size
	}
	return b.String()
}
