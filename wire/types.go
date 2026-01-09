package wire

type (
	// PeerID defines peer ID.
	PeerID [32]byte

	// Namespace defines namespace for messages.
	Namespace string

	// MessageID is the message ID returned from proton.
	MessageID uint64

	// Revision is the revision of the message used for deduplication.
	Revision uint64
)

// NamespaceRequest defines messages to receive.
type NamespaceRequest struct {
	Namespace  Namespace
	MessageIDs []MessageID
}

// Hello is the message exchanged between peers when connecting.
type Hello struct {
	PeerID   PeerID
	IsServer bool
	Requests []NamespaceRequest
}

// MessageDescriptor uniquely identifies type of exchanged message.
type MessageDescriptor struct {
	Namespace Namespace
	MessageID MessageID
}

// RevisionDescriptor uniquely identifies revision of message.
type RevisionDescriptor struct {
	Message MessageDescriptor
	Index   Revision
}

// Header describes the following message.
type Header struct {
	Sender   PeerID
	Revision RevisionDescriptor
}
