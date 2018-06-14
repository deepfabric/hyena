package raftstore

import (
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/uuid"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
	"github.com/infinivision/hyena/pkg/util"
)

type cmd struct {
	req   *raftpb.RaftCMDRequest
	cb    func(interface{})
	cbErr func([]byte, *raftpb.Error)
	term  uint64
}

func newCMD(req *reqCtx, pr *PeerReplicate) *cmd {
	c := acquireCmd()
	c.cb = req.cb
	c.cbErr = req.cbErr
	c.req = util.AcquireRaftCMDRequest()
	c.req.Admin = req.admin
	c.req.Header.ID = pr.id
	c.req.Header.Epoch = pr.ps.db.Epoch
	c.req.Header.Peer = pr.peer
	c.req.Header.UUID = uuid.NewV4().Bytes()

	c.append(req)
	return c
}

func (c *cmd) append(req *reqCtx) {
	if req.insert != nil {
		c.req.Inserts = append(c.req.Inserts, req.insert)
	} else if req.update != nil {
		c.req.Updates = append(c.req.Updates, req.update)
	} else if req.admin != nil {
		c.req.Admin = req.admin
	}
}

func (c *cmd) reset() {
	c.req = nil
	c.cb = nil
	c.term = 0
}

func (c *cmd) respError(err *raftpb.Error) {
	if c.cbErr == nil {
		return
	}

	log.Debugf("raftstore[db-%d]: response error to client, errors:\n%+v",
		c.req.Header.ID,
		err)

	for _, req := range c.req.Inserts {
		c.cbErr(req.ID, err)
	}

	for _, req := range c.req.Updates {
		c.cbErr(req.ID, err)
	}
}

func (c *cmd) respDBNotFound(id uint64) {
	c.respError(errorDBNotFound(id))
}

func (c *cmd) resp(resps ...interface{}) {
	if c.cb != nil {
		log.Debugf("raftstore[db-%d]: response to client",
			c.req.Header.ID)

		if len(c.req.Inserts) != len(resps) || len(c.req.Updates) != len(resps) {
			log.Fatalf("bug: requests and responses not match")
		}

		for _, resp := range resps {
			c.cb(resp)
		}

	}

	c.release()
}

func (c *cmd) release() {
	util.ReleaseRaftCMDRequest(c.req)
	for _, req := range c.req.Inserts {
		util.ReleaseInsertReq(req)
	}
	for _, req := range c.req.Updates {
		util.ReleaseUpdateReq(req)
	}
	releaseCmd(c)
}
