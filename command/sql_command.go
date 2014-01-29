package command

import (
	"github.com/coreos/raft"
	"stripe-ctf.com/sqlcluster/sql"
	"stripe-ctf.com/sqlcluster/log"
)

// This command executes SQL on the database
type SQLCommand struct {
	TxId  string `json:"txn"`
	Query string `json:"query"`
}

// Creates a new SQL command.
func NewSQLCommand(txId string, query string) *SQLCommand {
	return &SQLCommand{
		TxId:  txId,
		Query: query,
	}
}

// The name of the command in the log.
func (c *SQLCommand) CommandName() string {
	return "sql"
}

// Applies a SQL query.
func (c *SQLCommand) Apply(server raft.Server) (interface{}, error) {
	sqlDb := server.Context().(*sql.SQL)
	var out []byte

	if out = sqlDb.QueryCache[c.TxId]; out != nil {
		log.Printf("marikan Return cached SQL %s: %s", c.TxId, out)
		return out, nil
	}

	sqlDb.Begin()
	out, err := sqlDb.Execute(c.Query)
	sqlDb.Commit()
	sqlDb.QueryCache[c.TxId] = out
	log.Printf("marikan Applied SQL %s: %s", c.TxId, out)
	if err != nil {
		log.Printf("Error applying SQL!")
	}
	result := sql.Result{
		Output: out,
		Error:  err,
	}
	go func() {
		if ch, present := sqlDb.ResultChannels[c.TxId]; present {
			log.Printf("marikan Posting SQL results on %s", c.TxId)
			ch <- result
		}
	}()
	return out, err
}
