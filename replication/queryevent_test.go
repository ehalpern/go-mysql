package replication

import (
	"strings"
	"testing"
	"github.com/stretchr/testify/assert"
"github.com/siddontang/go/log"
)

func TestScanner(t *testing.T) {
	tokens := scanString("ALTER TABLE t1 ADD c1")
	assert.Equal(t, 5, len(tokens))
	tokens = scanString("ALTER TABLE 't1  1' ADD c1")
	assert.Equal(t, 5, len(tokens))
	assert.Equal(t, "t1  1", tokens[2])
	tokens = scanString("ALTER   TABLE t1\n ADD c1")
	assert.Equal(t, 5, len(tokens))
}

func scanString(s string) []string {
	scanner := NewQuotedScanner(strings.NewReader(s))
	var tokens []string
	for scanner.Scan() {
		tokens = append(tokens, scanner.Text())
	}
	return tokens
}

func TestParseQuery(t *testing.T) {
	q, err := ParseQuery("ALTER TABLE t1 ADD c1 VARCHAR(256)")
	log.Infof("query: %v", q)
	assert.Equal(t, "t1", q.Table)
	assert.Equal(t, "ADD", q.Operation)
	assert.Equal(t, "c1", q.Column)
	assert.Equal(t, "VARCHAR(256)", q.Type)

	q, err = ParseQuery("UPDATE TABLE t1 ADD c1 VARCHAR(256)")
	assert.Equal(t, ErrIgnored, err)
}
