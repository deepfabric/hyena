package server

import (
	"github.com/fagongzi/goetty"
)

type session struct {
	id   interface{}
	conn goetty.IOSession
}

func newSession(conn goetty.IOSession) *session {
	return &session{
		id:   conn.ID(),
		conn: conn,
	}
}

func (s *session) close() {

}
