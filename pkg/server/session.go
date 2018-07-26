package server

import (
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/infinivision/hyena/pkg/util"
)

const (
	batch = 32
)

var (
	closedFlag = struct{}{}
)

type session struct {
	id    interface{}
	conn  goetty.IOSession
	resps *util.Queue
}

func newSession(conn goetty.IOSession) *session {
	return &session{
		id:    conn.ID(),
		conn:  conn,
		resps: util.New(0),
	}
}

func (s *session) close() {
	s.resps.Put(closedFlag)
	log.Infof("session-[%v]: closed", s.id)
}

func (s *session) release() {
	s.resps.Dispose()
	s.conn.Close()
	log.Infof("session-[%v]: resource released", s.id)
}

func (s *session) resp(rsp interface{}) {
	s.resps.Put(rsp)
}

func (s *session) writeLoop() {
	items := make([]interface{}, batch, batch)

	for {
		n, err := s.resps.Get(batch, items)
		if nil != err {
			return
		}

		for i := int64(0); i < n; i++ {
			item := items[i]
			if item == closedFlag {
				s.release()
				return
			}
			s.conn.Write(items[i])
		}
		s.conn.Flush()
	}
}
