package wire

import (
	"reflect"
	"unsafe"

	"github.com/outofforest/proton"
	"github.com/outofforest/proton/helpers"
	"github.com/pkg/errors"
)

const (
	id4 uint64 = iota + 1
	id1
)

var _ proton.Marshaller = Marshaller{}

// NewMarshaller creates marshaller.
func NewMarshaller() Marshaller {
	return Marshaller{}
}

// Marshaller marshals and unmarshals messages.
type Marshaller struct {
}

// Messages returns list of the message types supported by marshaller.
func (m Marshaller) Messages() []any {
	return []any {
		Hello{},
		Header{},
	}
}

// ID returns ID of message type.
func (m Marshaller) ID(msg any) (uint64, error) {
	switch msg.(type) {
	case *Hello:
		return id4, nil
	case *Header:
		return id1, nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Size computes the size of marshalled message.
func (m Marshaller) Size(msg any) (uint64, error) {
	switch msg2 := msg.(type) {
	case *Hello:
		return size4(msg2), nil
	case *Header:
		return size1(msg2), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Marshal marshals message.
func (m Marshaller) Marshal(msg any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMarshal(&retErr)

	switch msg2 := msg.(type) {
	case *Hello:
		return id4, marshal4(msg2, buf), nil
	case *Header:
		return id1, marshal1(msg2, buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Unmarshal unmarshals message.
func (m Marshaller) Unmarshal(id uint64, buf []byte) (retMsg any, retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch id {
	case id4:
		msg := &Hello{}
		return msg, unmarshal4(msg, buf), nil
	case id1:
		msg := &Header{}
		return msg, unmarshal1(msg, buf), nil
	default:
		return nil, 0, errors.Errorf("unknown ID %d", id)
	}
}

// MakePatch creates a patch.
func (m Marshaller) MakePatch(msgDst, msgSrc any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMakePatch(&retErr)

	switch msg2 := msgDst.(type) {
	case *Hello:
		return id4, makePatch4(msg2, msgSrc.(*Hello), buf), nil
	case *Header:
		return id1, makePatch1(msg2, msgSrc.(*Header), buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msgDst)
	}
}

// ApplyPatch applies patch.
func (m Marshaller) ApplyPatch(msg any, buf []byte) (retSize uint64, retErr error) {
	defer helpers.RecoverApplyPatch(&retErr)

	switch msg2 := msg.(type) {
	case *Hello:
		return applyPatch4(msg2, buf), nil
	case *Header:
		return applyPatch1(msg2, buf), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

func size1(m *Header) uint64 {
	var n uint64 = 32
	{
		// Revision

		n += size0(&m.Revision)
	}
	return n
}

func marshal1(m *Header, b []byte) uint64 {
	var o uint64
	{
		// Sender

		copy(b[o:o+32], unsafe.Slice(&m.Sender[0], 32))
		o += 32
	}
	{
		// Revision

		o += marshal0(&m.Revision, b[o:])
	}

	return o
}

func unmarshal1(m *Header, b []byte) uint64 {
	var o uint64
	{
		// Sender

		copy(unsafe.Slice(&m.Sender[0], 32), b[o:o+32])
		o += 32
	}
	{
		// Revision

		o += unmarshal0(&m.Revision, b[o:])
	}

	return o
}

func makePatch1(m, mSrc *Header, b []byte) uint64 {
	var o uint64 = 1
	{
		// Sender

		if reflect.DeepEqual(m.Sender, mSrc.Sender) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			copy(b[o:o+32], unsafe.Slice(&m.Sender[0], 32))
			o += 32
		}
	}
	{
		// Revision

		if reflect.DeepEqual(m.Revision, mSrc.Revision) {
			b[0] &= 0xFD
		} else {
			b[0] |= 0x02
			o += marshal0(&m.Revision, b[o:])
		}
	}

	return o
}

func applyPatch1(m *Header, b []byte) uint64 {
	var o uint64 = 1
	{
		// Sender

		if b[0]&0x01 != 0 {
			copy(unsafe.Slice(&m.Sender[0], 32), b[o:o+32])
			o += 32
		}
	}
	{
		// Revision

		if b[0]&0x02 != 0 {
			o += unmarshal0(&m.Revision, b[o:])
		}
	}

	return o
}

func size0(m *RevisionDescriptor) uint64 {
	var n uint64 = 1
	{
		// Message

		n += size2(&m.Message)
	}
	{
		// Index

		helpers.UInt64Size(m.Index, &n)
	}
	return n
}

func marshal0(m *RevisionDescriptor, b []byte) uint64 {
	var o uint64
	{
		// Message

		o += marshal2(&m.Message, b[o:])
	}
	{
		// Index

		helpers.UInt64Marshal(m.Index, b, &o)
	}

	return o
}

func unmarshal0(m *RevisionDescriptor, b []byte) uint64 {
	var o uint64
	{
		// Message

		o += unmarshal2(&m.Message, b[o:])
	}
	{
		// Index

		helpers.UInt64Unmarshal(&m.Index, b, &o)
	}

	return o
}

func size2(m *MessageDescriptor) uint64 {
	var n uint64 = 2
	{
		// Namespace

		{
			l := uint64(len(m.Namespace))
			helpers.UInt64Size(l, &n)
			n += l
		}
	}
	{
		// MessageID

		helpers.UInt64Size(m.MessageID, &n)
	}
	return n
}

func marshal2(m *MessageDescriptor, b []byte) uint64 {
	var o uint64
	{
		// Namespace

		{
			l := uint64(len(m.Namespace))
			helpers.UInt64Marshal(l, b, &o)
			copy(b[o:o+l], m.Namespace)
			o += l
		}
	}
	{
		// MessageID

		helpers.UInt64Marshal(m.MessageID, b, &o)
	}

	return o
}

func unmarshal2(m *MessageDescriptor, b []byte) uint64 {
	var o uint64
	{
		// Namespace

		{
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.Namespace = Namespace(b[o:o+l])
				o += l
			}
		}
	}
	{
		// MessageID

		helpers.UInt64Unmarshal(&m.MessageID, b, &o)
	}

	return o
}

func size4(m *Hello) uint64 {
	var n uint64 = 34
	{
		// Requests

		l := uint64(len(m.Requests))
		helpers.UInt64Size(l, &n)
		for _, sv1 := range m.Requests {
			n += size3(&sv1)
		}
	}
	return n
}

func marshal4(m *Hello, b []byte) uint64 {
	var o uint64 = 1
	{
		// PeerID

		copy(b[o:o+32], unsafe.Slice(&m.PeerID[0], 32))
		o += 32
	}
	{
		// IsServer

		if m.IsServer {
			b[0] |= 0x01
		} else {
			b[0] &= 0xFE
		}
	}
	{
		// Requests

		helpers.UInt64Marshal(uint64(len(m.Requests)), b, &o)
		for _, sv1 := range m.Requests {
			o += marshal3(&sv1, b[o:])
		}
	}

	return o
}

func unmarshal4(m *Hello, b []byte) uint64 {
	var o uint64 = 1
	{
		// PeerID

		copy(unsafe.Slice(&m.PeerID[0], 32), b[o:o+32])
		o += 32
	}
	{
		// IsServer

		m.IsServer = b[0]&0x01 != 0
	}
	{
		// Requests

		var l uint64
		helpers.UInt64Unmarshal(&l, b, &o)
		if l > 0 {
			m.Requests = make([]NamespaceRequest, l)
			for i1 := range l {
				o += unmarshal3(&m.Requests[i1], b[o:])
			}
		}
	}

	return o
}

func makePatch4(m, mSrc *Hello, b []byte) uint64 {
	var o uint64 = 2
	{
		// PeerID

		if reflect.DeepEqual(m.PeerID, mSrc.PeerID) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			copy(b[o:o+32], unsafe.Slice(&m.PeerID[0], 32))
			o += 32
		}
	}
	{
		// IsServer

		if m.IsServer == mSrc.IsServer {
			b[1] &= 0xFE
		} else {
			b[1] |= 0x01
		}
	}
	{
		// Requests

		if reflect.DeepEqual(m.Requests, mSrc.Requests) {
			b[0] &= 0xFD
		} else {
			b[0] |= 0x02
			helpers.UInt64Marshal(uint64(len(m.Requests)), b, &o)
			for _, sv1 := range m.Requests {
				o += marshal3(&sv1, b[o:])
			}
		}
	}

	return o
}

func applyPatch4(m *Hello, b []byte) uint64 {
	var o uint64 = 2
	{
		// PeerID

		if b[0]&0x01 != 0 {
			copy(unsafe.Slice(&m.PeerID[0], 32), b[o:o+32])
			o += 32
		}
	}
	{
		// IsServer

		if b[1]&0x01 != 0 {
			m.IsServer = !m.IsServer
		}
	}
	{
		// Requests

		if b[0]&0x02 != 0 {
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.Requests = make([]NamespaceRequest, l)
				for i1 := range l {
					o += unmarshal3(&m.Requests[i1], b[o:])
				}
			}
		}
	}

	return o
}

func size3(m *NamespaceRequest) uint64 {
	var n uint64 = 2
	{
		// Namespace

		{
			l := uint64(len(m.Namespace))
			helpers.UInt64Size(l, &n)
			n += l
		}
	}
	{
		// MessageIDs

		l := uint64(len(m.MessageIDs))
		helpers.UInt64Size(l, &n)
		n += l
		for _, sv1 := range m.MessageIDs {
			helpers.UInt64Size(sv1, &n)
		}
	}
	return n
}

func marshal3(m *NamespaceRequest, b []byte) uint64 {
	var o uint64
	{
		// Namespace

		{
			l := uint64(len(m.Namespace))
			helpers.UInt64Marshal(l, b, &o)
			copy(b[o:o+l], m.Namespace)
			o += l
		}
	}
	{
		// MessageIDs

		helpers.UInt64Marshal(uint64(len(m.MessageIDs)), b, &o)
		for _, sv1 := range m.MessageIDs {
			helpers.UInt64Marshal(sv1, b, &o)
		}
	}

	return o
}

func unmarshal3(m *NamespaceRequest, b []byte) uint64 {
	var o uint64
	{
		// Namespace

		{
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.Namespace = Namespace(b[o:o+l])
				o += l
			}
		}
	}
	{
		// MessageIDs

		var l uint64
		helpers.UInt64Unmarshal(&l, b, &o)
		if l > 0 {
			m.MessageIDs = make([]MessageID, l)
			for i1 := range l {
				helpers.UInt64Unmarshal(&m.MessageIDs[i1], b, &o)
			}
		}
	}

	return o
}
