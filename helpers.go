package wave

import (
	"crypto/rand"
	"reflect"

	"github.com/pkg/errors"

	"github.com/outofforest/proton"
	"github.com/outofforest/wave/wire"
)

func peerID() (wire.PeerID, error) {
	var id wire.PeerID
	_, err := rand.Read(id[:])
	if err != nil {
		return wire.PeerID{}, errors.WithStack(err)
	}
	return id, nil
}

func marshallerToNamespace(m proton.Marshaller) wire.Namespace {
	t := reflect.TypeOf(m)
	return wire.Namespace(t.PkgPath() + "." + t.Name())
}
