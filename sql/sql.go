package sql

import (
	"fmt"
	"bytes"
	"strings"
	"database/sql"
	"stripe-ctf.com/sqlcluster/log"
	_ "github.com/mattn/go-sqlite3"
	"sync"
)

type Result struct {
	Output []byte
	Error  error
}

type SQL struct {
	path           string
	db             *sql.DB
	tx             *sql.Tx
	sequenceNumber int
	mutex          sync.Mutex
	ResultChannels  map[string]chan Result
}

func New(path string) (*SQL, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	s := &SQL{
		db:      db,
		path:    path,
		ResultChannels: make(map[string]chan Result),
	}

	return s, nil
}

func (s *SQL) executeQueries(queries string) (*bytes.Buffer, *bytes.Buffer) {
	var errorLine, prevErrorLine string
    stdout := new(bytes.Buffer)
    stderr := new(bytes.Buffer)
    lineNo := 1
	for _, q := range strings.Split(queries, ";") {
		for i := 0; i < len(q) && q[i] == '\n'; i++ {
			lineNo += 1
		}
		currentLineNo := lineNo
		q = strings.TrimLeft(q, "\r\n\t ")
		lineNo += strings.Count(q, "\n")
		q = strings.TrimRight(q, "\r\n\t ")
		if len(q) == 0 {
			continue
		}

		// 2014/01/29 14:27:24 [node3] 2014/01/29 14:27:24 [follower] Forwarding query: "UPDATE ctf3 SET friendCount=friendCount+84, requestCount=requestCount+1, favoriteWord=\"cqhfdgccagqhphj\" WHERE name=\"carl\"; SELECT * FROM ctf3;"
		// 2014/01/29 14:27:25 [node0] 2014/01/29 14:27:25 [leader] Executing query: "UPDATE ctf3 SET friendCount=friendCount+84, requestCount=requestCount+1, favoriteWord=\"cqhfdgccagqhphj\" WHERE name=\"carl\"; SELECT * FROM ctf3;"
		// 2014/01/29 14:27:25 [node0] 2014/01/29 14:27:25 [32] Query: UPDATE ctf3 SET friendCount=friendCount+84, requestCount=requestCount+1, favoriteWord="cqhfdgccagqhphj" WHERE name="carl"
		// 2014/01/29 14:27:25 [node0] 2014/01/29 14:27:25 Executed, affected rows: 1, last insert ID: 5
		// 2014/01/29 14:27:25 [node0] 2014/01/29 14:27:25 [32] Query: SELECT * FROM ctf3

		log.Debugf("[%d] Query: %s", s.sequenceNumber, q)

		if (!strings.HasPrefix(q, "SELECT ")) {
			res, err := s.db.Exec(q)
			if err != nil {
				log.Printf("Error excuting query: %s", err.Error())
				errorLine = fmt.Sprintf("Error: near line %d: %s\n",
					                    currentLineNo, err.Error())
				if (errorLine != prevErrorLine) {
					stderr.WriteString(errorLine)
					prevErrorLine = errorLine
				}
				continue
			}
			rowsAffected, _ := res.RowsAffected()
			lastInsertId, _ := res.LastInsertId()
			log.Debugf("Executed, affected rows: %d, last insert ID: %d",
				       rowsAffected, lastInsertId)
			continue
		}

		rows, err := s.db.Query(q)
		if err != nil {
			log.Printf("Error excuting query: %s", err.Error())
			errorLine = fmt.Sprintf("Error: near line %d: %s\n",
				                    currentLineNo, err.Error())
			if (errorLine != prevErrorLine) {
				stderr.WriteString(errorLine)
				prevErrorLine = errorLine
			}
			continue
		}

		cols, err := rows.Columns()
		if err != nil {
			panic(err)
		}

		if len(cols) == 0 {
			rows.Close()
			continue
		}

	    readCols := make([]interface{}, len(cols))
	    writeCols := make([]string, len(cols))
	    for i, _ := range writeCols {
	        readCols[i] = &writeCols[i]
	    }

		for rows.Next() {
	        if err := rows.Scan(readCols...); err != nil {
	            panic(err)
	        }
	        stdout.WriteString(strings.Join(writeCols, "|"))
			stdout.WriteByte(0x0a)
		}
		rows.Close()
	}

	return stdout, stderr
}

func (s *SQL) Begin() error {
	s.mutex.Lock()
	tx, err := s.db.Begin()
	s.tx = tx
	return err
}

func (s *SQL) Commit() error {
	err := s.tx.Commit()
	s.tx = nil
	s.sequenceNumber += 1
	s.mutex.Unlock()
	return err
}

func (s *SQL) Rollback() error {
	err := s.tx.Rollback()
	s.tx = nil
	s.mutex.Unlock()
	return err
}

func (s *SQL) Execute(queries string) ([]byte, error) {
	stdout, stderr := s.executeQueries(queries)

	formatted := fmt.Sprintf("SequenceNumber: %d\n%s%s",
		s.sequenceNumber, stdout.String(), stderr.String())
	return []byte(formatted), nil
}
