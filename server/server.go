package server

import (
	"fmt"

	"github.com/gh-efforts/retrieve-server/db"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("server")

type Server struct {
	d *db.DB
}

func New(d *db.DB) *Server {
	return &Server{
		d: d,
	}
}

func (s *Server) upsert(rb *RootBlock) error {
	var query string
	switch s.d.DBType {
	case "sqlite":
		query = `INSERT OR IGNORE INTO RootBlocks(root, size, block) VALUES (?, ?, ?)`
	case "postgres", "yugabyte":
		query = `INSERT INTO RootBlocks(root, size, block) VALUES ($1, $2, $3) ON CONFLICT (root) DO UPDATE SET size = $2, block = $3`
	default:
		return fmt.Errorf("unknown db type: %s", s.d.DBType)
	}

	_, err := s.d.DB.Exec(query, rb.Root, len(rb.Block), rb.Block)
	if err != nil {
		return err
	}

	log.Debugw("upsert", "root", rb.Root, "size", len(rb.Block))
	return nil
}

func (s *Server) delete(root string) error {
	_, err := s.d.DB.Exec(`DELETE FROM RootBlocks WHERE root=$1`, root)
	if err != nil {
		return err
	}

	log.Debugw("delete", "root", root)
	return nil
}

func (s *Server) block(root string) ([]byte, error) {
	var block []byte
	err := s.d.DB.QueryRow(`SELECT block FROM RootBlocks WHERE root=$1`, root).Scan(&block)
	if err != nil {
		return nil, err
	}

	log.Debugw("getblock", "root", root, "size", len(block))
	return block, nil
}

func (s *Server) size(root string) (int, error) {
	var size int
	err := s.d.DB.QueryRow(`SELECT size FROM RootBlocks WHERE root=$1`, root).Scan(&size)
	if err != nil {
		return 0, err
	}

	log.Debugw("getsize", "root", root, "size", size)
	return size, nil
}
