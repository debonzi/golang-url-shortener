package main

import (
	"database/sql"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"github.com/pkg/errors"

	_ "github.com/lib/pq"
)

var (
	entryPathPrefix     = "entry:"       // prefix for path-to-url mappings
	entryUserPrefix     = "user:"        // prefix for path-to-user mappings
	userToEntriesPrefix = "userEntries:" // prefix for user-to-[]entries mappings (redis SET)
	entryVisitsPrefix   = "entryVisits:" // prefix for entry-to-[]visit mappings (redis LIST)
)

// PsqlStore implements the stores.Storage interface
type PsqlStore struct {
	db *sql.DB
}

func InitDatabase(db *sql.DB) error {
	const kvschem = `
CREATE TABLE IF NOT EXISTS kvstore(
	key        TEXT PRIMARY KEY,
	value      JSONB,
	expires_at TIMESTAMPTZ
)`
	_, err := db.Exec(kvschem)
	return err
}

func PsqlNew(uri string) (*PsqlStore, error) {
	var db *sql.DB
	var err error
	if db, err = sql.Open("postgres", uri); err != nil {
		return nil, errors.Wrap(err, "Could not connect to psql")
	}
	if InitDatabase(db) != nil {
		return nil, errors.Wrap(err, "Could not initialize DB")
	}

	ret := &PsqlStore{db: db}
	return ret, err
}

func (p *PsqlStore) PsqlDisconnect() {
	defer p.db.Close()
}

func (p *PsqlStore) Insert(key string, raw []byte) error{
	const insert_string = `INSERT INTO kvstore(key, value) VALUES($1, $2)`
	_, err := p.db.Query(insert_string, key, raw)
	return err
}







type Message struct {
    Name string
    Body string
}

func main() {
	connStr := "postgres://geru:geru@localhost/pggus"  // "?sslmode=verify-full"
	p, err := PsqlNew(connStr)
	m := Message{"Debonzi", "Daniel"}
	raw, err := json.Marshal(m)
	if err != nil {
		logrus.Error(err)
	}
	p.Insert("debonzi", raw)
	p.PsqlDisconnect()
}
