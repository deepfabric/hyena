package codec

import (
	"fmt"

	"github.com/fagongzi/goetty"
	pbutil "github.com/fagongzi/util/protoc"
	"github.com/infinivision/hyena/pkg/pb/rpc"
	"github.com/infinivision/hyena/pkg/util"
)

var (
	c = &codec{}
)

// GetEncoder return encoder
func GetEncoder() goetty.Encoder {
	return c
}

// GetDecoder return decoder
func GetDecoder() goetty.Decoder {
	return c
}

type pbValue interface {
	pbutil.PB
	Size() int
}

type codec struct{}

func (c *codec) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	data := in.GetMarkedRemindData()
	msgType := rpc.MsgType(data[0])
	var value pbutil.PB

	switch msgType {
	case rpc.MsgSearchReq:
		value = util.AcquireSearchReq()
		break
	case rpc.MsgSearchRsp:
		value = util.AcquireSearchRsp()
		break
	case rpc.MsgInsertReq:
		value = util.AcquireInsertReq()
		break
	case rpc.MsgInsertRsp:
		value = util.AcquireInsertRsp()
		break
	case rpc.MsgUpdateReq:
		value = util.AcquireUpdateReq()
		break
	case rpc.MsgUpdateRsp:
		value = util.AcquireUpdateRsp()
		break
	default:
		return false, nil, fmt.Errorf("not support msg type: %d", data[0])
	}

	err := value.Unmarshal(data[1:])
	in.MarkedBytesReaded()
	if err != nil {
		return false, nil, err
	}

	return true, value, nil
}

func (c *codec) Encode(data interface{}, out *goetty.ByteBuf) error {
	var target pbValue

	switch t := data.(type) {
	case *rpc.SearchRequest:
		out.WriteByte(byte(rpc.MsgSearchReq))
		target = t
		break
	case *rpc.SearchResponse:
		out.WriteByte(byte(rpc.MsgSearchRsp))
		target = t
		break
	case *rpc.InsertRequest:
		out.WriteByte(byte(rpc.MsgInsertReq))
		target = t
	case *rpc.InsertResponse:
		out.WriteByte(byte(rpc.MsgInsertRsp))
		target = t
		break
	case *rpc.UpdateRequest:
		out.WriteByte(byte(rpc.MsgUpdateReq))
		target = t
	case *rpc.UpdateResponse:
		out.WriteByte(byte(rpc.MsgUpdateRsp))
		target = t
		break
	}

	size := target.Size()
	if size > 0 {
		index := out.GetWriteIndex()
		out.Expansion(size)
		target.MarshalTo(out.RawBuf()[index : index+size])
		out.SetWriterIndex(index + size)
	}

	return nil
}
