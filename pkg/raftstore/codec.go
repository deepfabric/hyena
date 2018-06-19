package raftstore

import (
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	pbutil "github.com/fagongzi/util/protoc"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
	"github.com/infinivision/hyena/pkg/util"
)

const (
	typeRaft byte = 1
	typeSnap byte = 2
)

var (
	decoder = goetty.NewIntLengthFieldBasedDecoder(&codec{})
	encoder = goetty.NewIntLengthFieldBasedEncoder(&codec{})
)

type codec struct{}

func (c *codec) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	t, err := in.ReadByte()
	if err != nil {
		return true, nil, err
	}

	data := in.GetMarkedRemindData()
	switch t {
	case typeSnap:
		msg := &raftpb.SnapshotMessage{}
		pbutil.MustUnmarshal(msg, data)
		in.MarkedBytesReaded()
		return true, msg, nil
	case typeRaft:
		msg := util.AcquireRaftMessage()
		pbutil.MustUnmarshal(msg, data)
		in.MarkedBytesReaded()
		return true, msg, nil
	}

	log.Fatalf("bug: not support msg type, type=<%d>", t)
	return false, nil, nil
}

func (c *codec) Encode(data interface{}, out *goetty.ByteBuf) error {
	size := 0
	t := typeRaft
	var m pbutil.PB

	if msg, ok := data.(*raftpb.RaftMessage); ok {
		t = typeRaft
		m = msg
		size = msg.Size()
	} else if msg, ok := data.(*raftpb.SnapshotMessage); ok {
		t = typeSnap
		m = msg
		size = msg.Size()
	} else {
		log.Fatalf("bug: unsupport msg: %+v", msg)
	}

	out.WriteByte(byte(t))
	if size > 0 {
		index := out.GetWriteIndex()
		out.Expansion(size)
		pbutil.MustMarshalTo(m, out.RawBuf()[index:index+size])
		out.SetWriterIndex(index + size)
	}

	return nil
}
