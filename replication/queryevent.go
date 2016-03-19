package replication

import (
	"bufio"
	"io"
	"strings"
	"github.com/siddontang/go/log"
	"github.com/juju/errors"
)

var (
	ErrIgnored = errors.New("Query event ignored")
)

// Creates a scanner that splits on words or quoted strings
func NewQuotedScanner(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		advance, token, err = bufio.ScanWords(data, atEOF)
		if err == nil && token != nil {
			firstChar := token[0]
			switch (firstChar) {
			case '\'', '"', '`':
				n := strings.Index(string(data[advance:]), string(firstChar))
				if n == -1 {
					token = append(token, data[advance:]...)
					advance = len(token)
				} else {
					log.Debugf("token:'%v', advance: %d, data: '%v'", string(token), advance, string(data))
					token = append(token[1:], data[advance - 1:advance + n]...)
					advance = advance + n
				}
			default:
				// nothing to do
			}
		}
		return
	}
	scanner.Split(split)
	return scanner
}

type AlterOp string
const (
	ADD AlterOp = "ADD"
	MODIFY AlterOp = "MODIFY"
	DELETE AlterOp = "DROP"
)

type AlterTableQuery struct {
	String string
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
		return nil, ErrIgnored
	}
	return nil, errors.NotValidf("Unrecognized query '%v'", query)
}

func parseAlterTable(scanner *bufio.Scanner) (*AlterTableQuery, error) {
	query := new(AlterTableQuery)
	scanner.Scan(); query.Table = scanner.Text()
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
	scanner.Scan(); query.Type = scanner.Text()
	if (query.Type == "") {
		return nil, errors.NotValidf("Missing column type in '%v'", scanner)
	}
	scanner.Scan(); query.Type = scanner.Text()
	return query, nil
}
