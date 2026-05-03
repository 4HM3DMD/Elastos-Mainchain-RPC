package validate

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

const SatsPerELA int64 = 100_000_000

// SatsToELA converts satoshi int64 to display string matching Elastos Fixed64.String().
// Whole numbers: no decimal ("5000", "0"). Fractional: 8 decimal places ("59042.89975700").
func SatsToELA(sats int64) string {
	if sats%SatsPerELA == 0 {
		return strconv.FormatInt(sats/SatsPerELA, 10)
	}
	whole := sats / SatsPerELA
	frac := sats % SatsPerELA
	if frac < 0 {
		frac = -frac
	}
	return fmt.Sprintf("%d.%08d", whole, frac)
}

// ELAToSats parses a value string like "59042.89975700" into satoshis. No floats.
func ELAToSats(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty value string")
	}
	parts := strings.SplitN(s, ".", 2)
	whole, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid whole part: %w", err)
	}
	if len(parts) == 1 {
		return whole * SatsPerELA, nil
	}
	fracStr := parts[1]
	for len(fracStr) < 8 {
		fracStr += "0"
	}
	fracStr = fracStr[:8]
	frac, err := strconv.ParseInt(fracStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid fractional part: %w", err)
	}
	if whole < 0 {
		return whole*SatsPerELA - frac, nil
	}
	return whole*SatsPerELA + frac, nil
}

// ValidateAddress checks that an ELA address is exactly 34 alphanumeric characters.
func ValidateAddress(addr string) error {
	if len(addr) != 34 {
		return fmt.Errorf("invalid address, len != 34")
	}
	for _, c := range addr {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) {
			return fmt.Errorf("invalid address, non-alphanumeric character")
		}
	}
	return nil
}

// SanitizeLog strips control characters from user input before logging.
func SanitizeLog(s string) string {
	if len(s) > 200 {
		s = s[:200]
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r >= 32 && r != 127 {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// CleanMemo hex-decodes memo bytes and strips invalid UTF-8.
func CleanMemo(hexData string) string {
	if hexData == "" {
		return ""
	}
	bytes, err := hexDecode(hexData)
	if err != nil {
		return ""
	}
	return strings.ToValidUTF8(string(bytes), "")
}

func hexDecode(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd hex length")
	}
	out := make([]byte, len(s)/2)
	for i := 0; i < len(s); i += 2 {
		hi := hexVal(s[i])
		lo := hexVal(s[i+1])
		if hi < 0 || lo < 0 {
			return nil, fmt.Errorf("invalid hex char")
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
