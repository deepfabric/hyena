package raftstore

import (
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/uuid"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
	rpcpb "github.com/infinivision/hyena/pkg/pb/rpc"
	"github.com/infinivision/hyena/pkg/util"
)

type cmd struct {
	req  *raftpb.RaftCMDRequest
	cb   func(interface{})
	term uint64
}

func newCMD(req *reqCtx, pr *PeerReplicate) *cmd {
	c := acquireCmd()
	c.cb = req.cb
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

func (c *cmd) respError(err *rpcpb.ErrResponse) {
	if c.cb == nil {
		return
	}

	log.Debugf("raftstore[db-%d]: response error to client, errors:\n%+v",
		c.req.Header.ID,
		err)

	for _, req := range c.req.Inserts {
		c.cb(copyWithID(req.ID, err))
	}

	for _, req := range c.req.Updates {
		c.cb(copyWithID(req.ID, err))
	}

	if c.req.Admin != nil {
		c.cb(err)
	}

	c.release()
}

func (c *cmd) respDBNotFound(id uint64) {
	c.respError(errorDBNotFound(nil, id))
}

func (c *cmd) resp(resps ...interface{}) {
	if c.cb != nil && c.req.Admin == nil {
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
