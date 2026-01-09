package wave

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/outofforest/logger"
	"github.com/outofforest/parallel"
	"github.com/outofforest/proton"
	"github.com/outofforest/resonance"
	"github.com/outofforest/wave/wire"
)

type msgToSend struct {
	Header     *wire.Header
	Marshaller proton.Marshaller
	Message    any
}

type clientConns struct {
	clientID wire.PeerID
	recvCh   chan<- any

	mu           sync.RWMutex
	conns        map[<-chan msgToSend]chan<- msgToSend
	sentMsgs     map[wire.MessageDescriptor]msgToSend
	receivedMsgs map[revDescriptor]wire.Revision
}

func newClientConns(clientID wire.PeerID, recvCh chan<- any) *clientConns {
	return &clientConns{
		clientID:     clientID,
		recvCh:       recvCh,
		conns:        map[<-chan msgToSend]chan<- msgToSend{},
		sentMsgs:     map[wire.MessageDescriptor]msgToSend{},
		receivedMsgs: map[revDescriptor]wire.Revision{},
	}
}

func (c *clientConns) Add() <-chan msgToSend {
	ch := make(chan msgToSend, 10)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.conns[ch] = ch

	for _, m := range c.sentMsgs {
		ch <- m
	}

	return ch
}

func (c *clientConns) Remove(ch <-chan msgToSend) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ch2, exists := c.conns[ch]; exists {
		delete(c.conns, ch)
		close(ch2)
	}
}

func (c *clientConns) Broadcast(msg any, marshaller proton.Marshaller) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	msgID, err := marshaller.ID(msg)
	if err != nil {
		return err
	}

	msgDescriptor := wire.MessageDescriptor{
		Namespace: marshallerToNamespace(marshaller),
		MessageID: wire.MessageID(msgID),
	}

	var revIndex wire.Revision
	if prevMsg, exists := c.sentMsgs[msgDescriptor]; exists {
		revIndex = prevMsg.Header.Revision.Index + 1
	}

	send := msgToSend{
		Header: &wire.Header{
			Sender: c.clientID,
			Revision: wire.RevisionDescriptor{
				Message: msgDescriptor,
				Index:   revIndex,
			},
		},
		Marshaller: marshaller,
		Message:    msg,
	}

	c.sentMsgs[msgDescriptor] = send

	for _, ch := range c.conns {
		ch <- send
	}

	return nil
}

func (c *clientConns) Deliver(ctx context.Context, header *wire.Header, msg any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	revDesc := revDescriptor{
		MessageDescriptor: header.Revision.Message,
		Sender:            header.Sender,
	}

	if existingRevision, exists := c.receivedMsgs[revDesc]; exists &&
		existingRevision >= header.Revision.Index {
		return nil
	}

	c.receivedMsgs[revDesc] = header.Revision.Index

	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case c.recvCh <- msg:
	}

	return nil
}

// ClientConfig is the config of client.
type ClientConfig struct {
	Servers        []string
	MaxMessageSize uint64
	Requests       []RequestConfig
}

// RequestConfig defines message types to receive on client.
type RequestConfig struct {
	Marshaller proton.Marshaller
	Messages   []any
}

// Client receives and sends requested messages from/to servers.
type Client struct {
	config      ClientConfig
	requests    []wire.NamespaceRequest
	marshallers map[wire.Namespace]proton.Marshaller
	conns       *clientConns
}

// NewClient creates new client.
func NewClient(config ClientConfig) (*Client, <-chan any, error) {
	if len(config.Servers) == 0 {
		return nil, nil, errors.New("no servers specified")
	}

	clientID, err := peerID()
	if err != nil {
		return nil, nil, err
	}

	marshallers := map[wire.Namespace]proton.Marshaller{}
	requests := make([]wire.NamespaceRequest, 0, len(config.Requests))
	for _, r := range config.Requests {
		namespace := marshallerToNamespace(r.Marshaller)
		if _, exists := marshallers[namespace]; !exists {
			marshallers[namespace] = r.Marshaller
		}

		req := wire.NamespaceRequest{
			Namespace:  namespace,
			MessageIDs: make([]wire.MessageID, 0, len(r.Messages)),
		}
		for _, m := range r.Messages {
			msgID, err := r.Marshaller.ID(m)
			if err != nil {
				return nil, nil, err
			}
			req.MessageIDs = append(req.MessageIDs, wire.MessageID(msgID))
		}

		requests = append(requests, req)
	}

	recvCh := make(chan any, 10)
	return &Client{
		config:      config,
		requests:    requests,
		marshallers: marshallers,
		conns:       newClientConns(clientID, recvCh),
	}, recvCh, nil
}

// Run runs client.
func (client *Client) Run(ctx context.Context) error {
	defer close(client.conns.recvCh)

	connConfig := resonance.Config{
		MaxMessageSize: client.config.MaxMessageSize,
	}

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		for _, server := range client.config.Servers {
			spawn("conn", parallel.Fail, func(ctx context.Context) error {
				log := logger.Get(ctx)

				for {
					err := resonance.RunClient(ctx, server, connConfig,
						func(ctx context.Context, c *resonance.Connection) error {
							return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
								return client.runConn(ctx, c)
							})
						})

					if ctx.Err() != nil {
						return errors.WithStack(ctx.Err())
					}

					log.Error("Wave connection failed", zap.String("server", server), zap.Error(err))
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

// Send sends new message to servers.
func (client *Client) Send(message any, marhsaller proton.Marshaller) error {
	return client.conns.Broadcast(message, marhsaller)
}

func (client *Client) runConn(ctx context.Context, c *resonance.Connection) error {
	m := wire.NewMarshaller()

	if err := c.SendProton(&wire.Hello{
		PeerID:   client.conns.clientID,
		Requests: client.requests,
	}, m); err != nil {
		return err
	}

	msg, err := c.ReceiveProton(m)
	if err != nil {
		return err
	}

	_, ok := msg.(*wire.Hello)
	if !ok {
		return errors.New("hello message expected")
	}

	sendCh := client.conns.Add()

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("receiver", parallel.Fail, func(ctx context.Context) error {
			defer client.conns.Remove(sendCh)

			for {
				msg, err := c.ReceiveProton(m)
				if err != nil {
					return err
				}

				headerMsg, ok := msg.(*wire.Header)
				if !ok {
					return errors.New("header message expected")
				}

				msgM, exists := client.marshallers[headerMsg.Revision.Message.Namespace]
				if !exists {
					return errors.Errorf("no marshaller for namespace %q", headerMsg.Revision.Message.Namespace)
				}

				msg, err = c.ReceiveProton(msgM)
				if err != nil {
					return err
				}

				if err := client.conns.Deliver(ctx, headerMsg, msg); err != nil {
					return err
				}
			}
		})
		spawn("sender", parallel.Fail, func(ctx context.Context) error {
			defer func() {
				for range sendCh {
				}
			}()
			defer c.Close()

			for toSend := range sendCh {
				if err := c.SendProton(toSend.Header, m); err != nil {
					return err
				}
				if err := c.SendProton(toSend.Message, toSend.Marshaller); err != nil {
					return err
				}
			}

			return nil
		})

		return nil
	})
}
