package wave

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/parallel"
	"github.com/outofforest/resonance"
	"github.com/outofforest/wave/wire"
)

var errSameServer = errors.New("connected to myself")

type revDescriptor struct {
	wire.MessageDescriptor

	Sender wire.PeerID
}

type revision struct {
	Header  *wire.Header
	Content []byte
}

type chans struct {
	Sender   chan<- revision
	Receiver <-chan revision
}

type serverConns struct {
	mu    sync.RWMutex
	conns map[wire.PeerID]chans
	msgs  map[revDescriptor]revision
}

func newServerConns() *serverConns {
	return &serverConns{
		conns: map[wire.PeerID]chans{},
		msgs:  map[revDescriptor]revision{},
	}
}

func (c *serverConns) Add(peerID wire.PeerID) <-chan revision {
	ch := make(chan revision, 10)

	c.mu.Lock()
	defer c.mu.Unlock()

	if ch, ok := c.conns[peerID]; ok {
		close(ch.Sender)
	}

	c.conns[peerID] = chans{Sender: ch, Receiver: ch}

	for _, m := range c.msgs {
		ch <- m
	}

	return ch
}

func (c *serverConns) Remove(peerID wire.PeerID, ch <-chan revision) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if chs, exists := c.conns[peerID]; exists && chs.Receiver == ch {
		delete(c.conns, peerID)
		close(chs.Sender)
	}
}

func (c *serverConns) Broadcast(msgRev revision) {
	c.mu.Lock()
	defer c.mu.Unlock()

	revDesc := revDescriptor{
		MessageDescriptor: msgRev.Header.Revision.Message,
		Sender:            msgRev.Header.Sender,
	}

	if existingRevision, exists := c.msgs[revDesc]; exists &&
		existingRevision.Header.Revision.Index >= msgRev.Header.Revision.Index {
		return
	}

	c.msgs[revDesc] = msgRev

	for _, conn := range c.conns {
		conn.Sender <- msgRev
	}
}

// ServerConfig defines server configuration.
type ServerConfig struct {
	Servers        []string
	MaxMessageSize uint64
}

// RunServer runs server.
func RunServer(ctx context.Context, ls net.Listener, config ServerConfig) error {
	serverID, err := peerID()
	if err != nil {
		return err
	}

	conns := newServerConns()
	connConfig := resonance.Config{
		MaxMessageSize: config.MaxMessageSize,
	}

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) (err error) {
		spawn("server", parallel.Fail, func(ctx context.Context) error {
			return resonance.RunServer(ctx, ls, connConfig,
				func(ctx context.Context, c *resonance.Connection) error {
					return runServerConn(ctx, serverID, c, conns)
				})
		})

		for _, s := range config.Servers {
			spawn("client", parallel.Continue, func(ctx context.Context) error {
				log := logger.Get(ctx)

				for {
					err := resonance.RunClient(ctx, s, connConfig,
						func(ctx context.Context, c *resonance.Connection) error {
							return runServerConn(ctx, serverID, c, conns)
						})

					if ctx.Err() != nil {
						return errors.WithStack(ctx.Err())
					}

					if errors.Is(err, errSameServer) {
						return nil
					}

					log.Error("Wave connection failed", zap.String("server", s), zap.Error(err))
					select {
					case <-ctx.Done():
						return errors.WithStack(ctx.Err())
					case <-time.After(time.Second):
					}
				}
			})
		}

		return nil
	})
}

func runServerConn(
	ctx context.Context,
	serverID wire.PeerID,
	c *resonance.Connection,
	conns *serverConns,
) error {
	m := wire.NewMarshaller()

	if err := c.SendProton(&wire.Hello{
		PeerID:   serverID,
		IsServer: true,
	}, m); err != nil {
		return err
	}

	msg, err := c.ReceiveProton(m)
	if err != nil {
		return err
	}

	helloMsg, ok := msg.(*wire.Hello)
	if !ok {
		return errors.New("hello message expected")
	}

	if helloMsg.PeerID == serverID {
		return errSameServer
	}

	reqs := map[wire.MessageDescriptor]struct{}{}
	for _, r := range helloMsg.Requests {
		for _, mID := range r.MessageIDs {
			reqs[wire.MessageDescriptor{
				Namespace: r.Namespace,
				MessageID: mID,
			}] = struct{}{}
		}
	}

	sendCh := conns.Add(helloMsg.PeerID)

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("receiver", parallel.Fail, func(ctx context.Context) error {
			defer conns.Remove(helloMsg.PeerID, sendCh)

			for {
				msg, err := c.ReceiveProton(m)
				if err != nil {
					return err
				}

				headerMsg, ok := msg.(*wire.Header)
				if !ok {
					return errors.New("header message expected")
				}

				contentMsg, err := c.ReceiveRawBytes()
				if err != nil {
					return err
				}

				conns.Broadcast(revision{
					Header:  headerMsg,
					Content: contentMsg,
				})
			}
		})
		spawn("sender", parallel.Fail, func(ctx context.Context) error {
			defer func() {
				for range sendCh {
				}
			}()
			defer c.Close()

			for msgRev := range sendCh {
				if _, exists := reqs[msgRev.Header.Revision.Message]; !exists && !helloMsg.IsServer {
					continue
				}

				if err := c.SendProton(msgRev.Header, m); err != nil {
					return err
				}
				if err := c.SendRawBytes(msgRev.Content); err != nil {
					return err
				}
			}

			return nil
		})

		return nil
	})
}
