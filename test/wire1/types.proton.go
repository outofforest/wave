package wire1

import (
	"reflect"

	"github.com/outofforest/proton"
	"github.com/outofforest/proton/helpers"
	"github.com/pkg/errors"
)

const (
	id1 uint64 = iota + 1
	id0
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
		Msg1{},
		Msg2{},
	}
}

// ID returns ID of message type.
func (m Marshaller) ID(msg any) (uint64, error) {
	switch msg.(type) {
	case *Msg1:
		return id1, nil
	case *Msg2:
		return id0, nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Size computes the size of marshalled message.
func (m Marshaller) Size(msg any) (uint64, error) {
	switch msg2 := msg.(type) {
	case *Msg1:
		return size1(msg2), nil
	case *Msg2:
		return size0(msg2), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Marshal marshals message.
func (m Marshaller) Marshal(msg any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMarshal(&retErr)

	switch msg2 := msg.(type) {
	case *Msg1:
		return id1, marshal1(msg2, buf), nil
	case *Msg2:
		return id0, marshal0(msg2, buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msg)
	}
}

// Unmarshal unmarshals message.
func (m Marshaller) Unmarshal(id uint64, buf []byte) (retMsg any, retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch id {
	case id1:
		msg := &Msg1{}
		return msg, unmarshal1(msg, buf), nil
	case id0:
		msg := &Msg2{}
		return msg, unmarshal0(msg, buf), nil
	default:
		return nil, 0, errors.Errorf("unknown ID %d", id)
	}
}

// MakePatch creates a patch.
func (m Marshaller) MakePatch(msgDst, msgSrc any, buf []byte) (retID, retSize uint64, retErr error) {
	defer helpers.RecoverMakePatch(&retErr)

	switch msg2 := msgDst.(type) {
	case *Msg1:
		return id1, makePatch1(msg2, msgSrc.(*Msg1), buf), nil
	case *Msg2:
		return id0, makePatch0(msg2, msgSrc.(*Msg2), buf), nil
	default:
		return 0, 0, errors.Errorf("unknown message type %T", msgDst)
	}
}

// ApplyPatch applies patch.
func (m Marshaller) ApplyPatch(msg any, buf []byte) (retSize uint64, retErr error) {
	defer helpers.RecoverUnmarshal(&retErr)

	switch msg2 := msg.(type) {
	case *Msg1:
		return applyPatch1(msg2, buf), nil
	case *Msg2:
		return applyPatch0(msg2, buf), nil
	default:
		return 0, errors.Errorf("unknown message type %T", msg)
	}
}

func size0(m *Msg2) uint64 {
	var n uint64 = 1
	{
		// Value

		helpers.UInt64Size(m.Value, &n)
	}
	return n
}

func marshal0(m *Msg2, b []byte) uint64 {
	var o uint64
	{
		// Value

		helpers.UInt64Marshal(m.Value, b, &o)
	}

	return o
}

func unmarshal0(m *Msg2, b []byte) uint64 {
	var o uint64
	{
		// Value

		helpers.UInt64Unmarshal(&m.Value, b, &o)
	}

	return o
}

func makePatch0(m, mSrc *Msg2, b []byte) uint64 {
	var o uint64 = 1
	{
		// Value

		if reflect.DeepEqual(m.Value, mSrc.Value) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			helpers.UInt64Marshal(m.Value, b, &o)
		}
	}

	return o
}

func applyPatch0(m *Msg2, b []byte) uint64 {
	var o uint64 = 1
	{
		// Value

		if b[0]&0x01 != 0 {
			helpers.UInt64Unmarshal(&m.Value, b, &o)
		}
	}

	return o
}

func size1(m *Msg1) uint64 {
	var n uint64 = 1
	{
		// Value

		{
			l := uint64(len(m.Value))
			helpers.UInt64Size(l, &n)
			n += l
		}
	}
	return n
}

func marshal1(m *Msg1, b []byte) uint64 {
	var o uint64
	{
		// Value

		{
			l := uint64(len(m.Value))
			helpers.UInt64Marshal(l, b, &o)
			copy(b[o:o+l], m.Value)
			o += l
		}
	}

	return o
}

func unmarshal1(m *Msg1, b []byte) uint64 {
	var o uint64
	{
		// Value

		{
			var l uint64
			helpers.UInt64Unmarshal(&l, b, &o)
			if l > 0 {
				m.Value = string(b[o:o+l])
				o += l
			}
		}
	}

	return o
}

func makePatch1(m, mSrc *Msg1, b []byte) uint64 {
	var o uint64 = 1
	{
		// Value

		if reflect.DeepEqual(m.Value, mSrc.Value) {
			b[0] &= 0xFE
		} else {
			b[0] |= 0x01
			{
				l := uint64(len(m.Value))
				helpers.UInt64Marshal(l, b, &o)
				copy(b[o:o+l], m.Value)
				o += l
			}
		}
	}

	return o
}

func applyPatch1(m *Msg1, b []byte) uint64 {
	var o uint64 = 1
	{
		// Value

		if b[0]&0x01 != 0 {
			{
				var l uint64
				helpers.UInt64Unmarshal(&l, b, &o)
				if l > 0 {
					m.Value = string(b[o:o+l])
					o += l
				}
			}
		}
	}

	return o
}
