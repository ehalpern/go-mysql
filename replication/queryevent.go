package replication

import (
	"bufio"
	"io"
	"strings"
	"github.com/siddontang/go/log"
	"github.com/juju/errors"
	"unicode/utf8"
	"unicode"
)

var (
	ErrIgnored = errors.New("Query event ignored")
)

// Creates a scanner that splits on words or quoted strings
func NewQuotedScanner(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		// Skip leading spaces.
		start := 0
		for width := 0; start < len(data); start += width {
			var r rune
			r, width = utf8.DecodeRune(data[start:])
			if !unicode.IsSpace(r) {
				break
			}
		}

		// Does word start with a quote?
		quote, width := utf8.DecodeRune(data[start:])
		if IsQuote(quote) {
			log.Debugf("Quote detected '%c', advancing %d", quote, width)
			start = start + width
		} else {
			quote = 0
		}

		// Scan until space, marking end of word.
		for width, i := 0, start; i < len(data); i += width {
			var r rune
			r, width = utf8.DecodeRune(data[i:])
			if quote == 0 {
				if unicode.IsSpace(r) {
					return i + width, data[start:i], nil
				}
			} else {
				// Look for ending quote
				// BUG: need to implement escape handling
				if r == quote {
					log.Debugf("Found end quote %d chars after start", i)
					return i + width, data[start:i], nil
				}
			}
		}
		// If we're at EOF, we have a final, non-empty, non-terminated word. Return it.
		if atEOF && len(data) > start {
			return len(data), data[start:], nil
		}
		// Request more data.
		return start, nil, nil
	}
	scanner.Split(split)
	return scanner
}

func IsQuote(r rune) bool {
	switch r {
	case '\'', '"', '`':
		return true
	default:
		return false
	}
}

type AlterOp string
const (
	ADD AlterOp = "ADD"
	MODIFY AlterOp = "MODIFY"
	DELETE AlterOp = "DROP"
)

type AlterTableQuery struct {
	String string
	Schema string // "" if using the current schema
	Table string
	Operation AlterOp
	Column string
	Type string
	Extra string
}

func ParseQuery(query string) (*AlterTableQuery, error) {
	scanner := NewQuotedScanner(strings.NewReader(query))
	scanner.Scan()
	switch strings.ToUpper(scanner.Text()) {
	case "ALTER":
		if scanner.Scan(); strings.ToUpper(scanner.Text()) == "TABLE" {
			log.Info("Scanned TABLE")
			return parseAlterTable(scanner)
		}
	default:
		log.Infof("Ignoring query starting with: %v", scanner.Text())
		return nil, ErrIgnored
	}
	return nil, errors.NotValidf("Unrecognized query '%v'", query)
}

func parseAlterTable(scanner *bufio.Scanner) (*AlterTableQuery, error) {
	query := new(AlterTableQuery)
	scanner.Scan(); query.Table = scanner.Text()
	// Handle <schema>.<table>. Note this doesn't properly handle case where table is
	// quoted and '.' does not indicate a schema prefix
	if split := strings.Split(query.Table, "."); len(split) == 2 {
		query.Schema = split[0]
		query.Table = split[1]
	}
	scanner.Scan(); query.Operation = AlterOp(strings.ToUpper(scanner.Text()))
	switch query.Operation {
	case ADD, MODIFY, DELETE:
	default:
		return nil, errors.NotValidf("Unrecognized ALTER operation '%v' in '%v'", query.Operation, scanner)
	}
	scanner.Scan(); query.Column = scanner.Text()
	if (query.Column == "") {
		return nil, errors.NotValidf("Missing column name in '%v'", scanner)
	}
	scanner.Scan(); query.Type = strings.ToUpper(scanner.Text())
	if (query.Type == "") {
		return nil, errors.NotValidf("Missing column type in '%v'", scanner)
	}
	scanner.Scan(); query.Extra = strings.ToUpper(scanner.Text())
	return query, nil
}
