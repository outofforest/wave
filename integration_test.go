package wave_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/parallel"
	"github.com/outofforest/qa"
	"github.com/outofforest/wave"
	"github.com/outofforest/wave/test/wire1"
	"github.com/outofforest/wave/test/wire2"
)

const maxMsgSize = 1024

func TestSingleServerAndClient(t *testing.T) {
	requireT := require.New(t)

	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)

	defer func() {
		group.Exit(nil)
		requireT.NoError(group.Wait())
	}()

	ls, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)

	servers := []string{
		ls.Addr().String(),
	}

	m := wire1.NewMarshaller()
	clientConfig := wave.ClientConfig{
		Servers:        servers,
		MaxMessageSize: maxMsgSize,
		Requests: []wave.RequestConfig{
			{
				Marshaller: m,
				Messages: []any{
					&wire1.Msg1{},
				},
			},
		},
	}

	client, recvCh, err := wave.NewClient(clientConfig)
	requireT.NoError(err)

	group.Spawn("client", parallel.Fail, client.Run)
	group.Spawn("server", parallel.Fail, func(ctx context.Context) error {
		return wave.RunServer(ctx, ls, wave.ServerConfig{
			Servers:        servers,
			MaxMessageSize: maxMsgSize,
		})
	})

	requireT.NoError(client.Send(&wire1.Msg1{
		Value: "test1",
	}, m))

	testMsgs(ctx, requireT, recvCh,
		&wire1.Msg1{Value: "test1"},
	)

	requireT.NoError(client.Send(&wire1.Msg1{
		Value: "test2",
	}, m))

	testMsgs(ctx, requireT, recvCh,
		&wire1.Msg1{Value: "test2"},
	)
}

func TestOnlyRequestedMessagesAreReceived(t *testing.T) {
	requireT := require.New(t)

	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)

	defer func() {
		group.Exit(nil)
		requireT.NoError(group.Wait())
	}()

	ls, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)

	servers := []string{
		ls.Addr().String(),
	}

	m := wire1.NewMarshaller()
	clientConfig := wave.ClientConfig{
		Servers:        servers,
		MaxMessageSize: maxMsgSize,
		Requests: []wave.RequestConfig{
			{
				Marshaller: m,
				Messages: []any{
					&wire1.Msg2{},
				},
			},
		},
	}

	client, recvCh, err := wave.NewClient(clientConfig)
	requireT.NoError(err)

	group.Spawn("client", parallel.Fail, client.Run)
	group.Spawn("server", parallel.Fail, func(ctx context.Context) error {
		return wave.RunServer(ctx, ls, wave.ServerConfig{
			Servers:        servers,
			MaxMessageSize: maxMsgSize,
		})
	})

	requireT.NoError(client.Send(&wire1.Msg1{
		Value: "test",
	}, m))
	requireT.NoError(client.Send(&wire1.Msg2{
		Value: 2,
	}, m))

	testMsgs(ctx, requireT, recvCh,
		&wire1.Msg2{Value: 2},
	)
}

func TestTwoNamespaces(t *testing.T) {
	requireT := require.New(t)

	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)

	defer func() {
		group.Exit(nil)
		requireT.NoError(group.Wait())
	}()

	ls, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)

	servers := []string{
		ls.Addr().String(),
	}

	m1 := wire1.NewMarshaller()
	m2 := wire2.NewMarshaller()
	clientConfig := wave.ClientConfig{
		Servers:        servers,
		MaxMessageSize: maxMsgSize,
		Requests: []wave.RequestConfig{
			{
				Marshaller: m1,
				Messages: []any{
					&wire1.Msg2{},
				},
			},
			{
				Marshaller: m2,
				Messages: []any{
					&wire2.Msg2{},
				},
			},
		},
	}

	client, recvCh, err := wave.NewClient(clientConfig)
	requireT.NoError(err)

	group.Spawn("client", parallel.Fail, client.Run)
	group.Spawn("server", parallel.Fail, func(ctx context.Context) error {
		return wave.RunServer(ctx, ls, wave.ServerConfig{
			Servers:        servers,
			MaxMessageSize: maxMsgSize,
		})
	})

	requireT.NoError(client.Send(&wire1.Msg1{
		Value: "test1",
	}, m1))
	requireT.NoError(client.Send(&wire1.Msg2{
		Value: 1,
	}, m1))
	requireT.NoError(client.Send(&wire2.Msg1{
		Value: "test2",
	}, m2))
	requireT.NoError(client.Send(&wire2.Msg2{
		Value: 2,
	}, m2))

	testMsgs(ctx, requireT, recvCh,
		&wire1.Msg2{Value: 1},
		&wire2.Msg2{Value: 2},
	)
}

func TestServerSendsMessagesToNewClient(t *testing.T) {
	requireT := require.New(t)

	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)

	defer func() {
		group.Exit(nil)
		requireT.NoError(group.Wait())
	}()

	ls1, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)

	servers := []string{
		ls1.Addr().String(),
	}

	m := wire1.NewMarshaller()
	clientConfig := wave.ClientConfig{
		Servers:        servers,
		MaxMessageSize: maxMsgSize,
		Requests: []wave.RequestConfig{
			{
				Marshaller: m,
				Messages:   []any{&wire1.Msg1{}, &wire1.Msg2{}},
			},
		},
	}

	client1, recvCh1, err := wave.NewClient(clientConfig)
	requireT.NoError(err)

	client2, recvCh2, err := wave.NewClient(clientConfig)
	requireT.NoError(err)

	group.Spawn("client1", parallel.Fail, client1.Run)
	group.Spawn("server1", parallel.Fail, func(ctx context.Context) error {
		return wave.RunServer(ctx, ls1, wave.ServerConfig{
			Servers:        servers,
			MaxMessageSize: maxMsgSize,
		})
	})

	requireT.NoError(client1.Send(&wire1.Msg1{
		Value: "test1",
	}, m))

	testMsgs(ctx, requireT, recvCh1,
		&wire1.Msg1{Value: "test1"},
	)

	group.Spawn("client2", parallel.Fail, client2.Run)

	testMsgs(ctx, requireT, recvCh1)
	testMsgs(ctx, requireT, recvCh2,
		&wire1.Msg1{Value: "test1"},
	)

	requireT.NoError(client1.Send(&wire1.Msg1{
		Value: "test2",
	}, m))

	testMsgs(ctx, requireT, recvCh1,
		&wire1.Msg1{Value: "test2"},
	)
	testMsgs(ctx, requireT, recvCh2,
		&wire1.Msg1{Value: "test2"},
	)
}

func TestServersExchangeMessages(t *testing.T) {
	requireT := require.New(t)

	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)

	defer func() {
		group.Exit(nil)
		requireT.NoError(group.Wait())
	}()

	ls1, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	ls2, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)

	servers := []string{
		ls1.Addr().String(),
		ls2.Addr().String(),
	}

	m := wire1.NewMarshaller()
	clientConfig1 := wave.ClientConfig{
		Servers:        []string{ls1.Addr().String()},
		MaxMessageSize: maxMsgSize,
		Requests: []wave.RequestConfig{
			{
				Marshaller: m,
				Messages:   []any{&wire1.Msg2{}},
			},
		},
	}
	clientConfig2 := wave.ClientConfig{
		Servers:        []string{ls2.Addr().String()},
		MaxMessageSize: maxMsgSize,
		Requests: []wave.RequestConfig{
			{
				Marshaller: m,
				Messages:   []any{&wire1.Msg1{}},
			},
		},
	}

	client1, recvCh1, err := wave.NewClient(clientConfig1)
	requireT.NoError(err)

	client2, recvCh2, err := wave.NewClient(clientConfig2)
	requireT.NoError(err)

	group.Spawn("client1", parallel.Fail, client1.Run)
	group.Spawn("client2", parallel.Fail, client2.Run)
	group.Spawn("server1", parallel.Fail, func(ctx context.Context) error {
		return wave.RunServer(ctx, ls1, wave.ServerConfig{
			Servers:        servers,
			MaxMessageSize: maxMsgSize,
		})
	})
	group.Spawn("server2", parallel.Fail, func(ctx context.Context) error {
		return wave.RunServer(ctx, ls2, wave.ServerConfig{
			Servers:        servers,
			MaxMessageSize: maxMsgSize,
		})
	})

	requireT.NoError(client1.Send(&wire1.Msg1{
		Value: "test",
	}, m))
	requireT.NoError(client2.Send(&wire1.Msg2{
		Value: 1,
	}, m))

	testMsgs(ctx, requireT, recvCh1,
		&wire1.Msg2{Value: 1},
	)
	testMsgs(ctx, requireT, recvCh2,
		&wire1.Msg1{Value: "test"},
	)
}

func TestServerSynchronization(t *testing.T) {
	requireT := require.New(t)

	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)

	defer func() {
		group.Exit(nil)
		requireT.NoError(group.Wait())
	}()

	ls1, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	ls2, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)

	servers := []string{
		ls1.Addr().String(),
		ls2.Addr().String(),
	}

	m := wire1.NewMarshaller()
	clientConfig1 := wave.ClientConfig{
		Servers:        []string{ls1.Addr().String()},
		MaxMessageSize: maxMsgSize,
		Requests: []wave.RequestConfig{
			{
				Marshaller: m,
				Messages:   []any{&wire1.Msg2{}},
			},
		},
	}
	clientConfig2 := wave.ClientConfig{
		Servers:        []string{ls2.Addr().String()},
		MaxMessageSize: maxMsgSize,
		Requests: []wave.RequestConfig{
			{
				Marshaller: m,
				Messages:   []any{&wire1.Msg1{}},
			},
		},
	}

	client1, recvCh1, err := wave.NewClient(clientConfig1)
	requireT.NoError(err)

	client2, recvCh2, err := wave.NewClient(clientConfig2)
	requireT.NoError(err)

	group.Spawn("client1", parallel.Fail, client1.Run)
	group.Spawn("client2", parallel.Fail, client2.Run)
	group.Spawn("server1", parallel.Fail, func(ctx context.Context) error {
		return wave.RunServer(ctx, ls1, wave.ServerConfig{
			Servers:        servers,
			MaxMessageSize: maxMsgSize,
		})
	})

	requireT.NoError(client1.Send(&wire1.Msg1{
		Value: "test",
	}, m))
	requireT.NoError(client2.Send(&wire1.Msg2{
		Value: 1,
	}, m))

	group.Spawn("server2", parallel.Fail, func(ctx context.Context) error {
		return wave.RunServer(ctx, ls2, wave.ServerConfig{
			Servers:        servers,
			MaxMessageSize: maxMsgSize,
		})
	})

	testMsgs(ctx, requireT, recvCh1,
		&wire1.Msg2{Value: 1},
	)
	testMsgs(ctx, requireT, recvCh2,
		&wire1.Msg1{Value: "test"},
	)
}

func TestOnlyLatestRevisionIsSynced(t *testing.T) {
	requireT := require.New(t)

	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)

	defer func() {
		group.Exit(nil)
		requireT.NoError(group.Wait())
	}()

	ls1, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	ls2, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)

	servers := []string{
		ls1.Addr().String(),
		ls2.Addr().String(),
	}

	m := wire1.NewMarshaller()
	clientConfig1 := wave.ClientConfig{
		Servers:        []string{ls1.Addr().String()},
		MaxMessageSize: maxMsgSize,
		Requests: []wave.RequestConfig{
			{
				Marshaller: m,
				Messages:   []any{&wire1.Msg2{}},
			},
		},
	}
	clientConfig2 := wave.ClientConfig{
		Servers:        []string{ls2.Addr().String()},
		MaxMessageSize: maxMsgSize,
		Requests: []wave.RequestConfig{
			{
				Marshaller: m,
				Messages:   []any{&wire1.Msg1{}},
			},
		},
	}

	client1, recvCh1, err := wave.NewClient(clientConfig1)
	requireT.NoError(err)

	client2, recvCh2, err := wave.NewClient(clientConfig2)
	requireT.NoError(err)

	group.Spawn("client1", parallel.Fail, client1.Run)
	group.Spawn("client2", parallel.Fail, client2.Run)
	group.Spawn("server1", parallel.Fail, func(ctx context.Context) error {
		return wave.RunServer(ctx, ls1, wave.ServerConfig{
			Servers:        servers,
			MaxMessageSize: maxMsgSize,
		})
	})

	requireT.NoError(client1.Send(&wire1.Msg1{
		Value: "test1",
	}, m))
	requireT.NoError(client1.Send(&wire1.Msg1{
		Value: "test2",
	}, m))

	group.Spawn("server2", parallel.Fail, func(ctx context.Context) error {
		return wave.RunServer(ctx, ls2, wave.ServerConfig{
			Servers:        servers,
			MaxMessageSize: maxMsgSize,
		})
	})

	testMsgs(ctx, requireT, recvCh1)
	testMsgs(ctx, requireT, recvCh2,
		&wire1.Msg1{Value: "test2"},
	)
}

func TestSameMessagesFromTwoSourcesAreReceived(t *testing.T) {
	requireT := require.New(t)

	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)

	defer func() {
		group.Exit(nil)
		requireT.NoError(group.Wait())
	}()

	ls, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)

	servers := []string{
		ls.Addr().String(),
	}

	m := wire1.NewMarshaller()
	clientConfig := wave.ClientConfig{
		Servers:        servers,
		MaxMessageSize: maxMsgSize,
	}
	clientConfig3 := wave.ClientConfig{
		Servers:        servers,
		MaxMessageSize: maxMsgSize,
		Requests: []wave.RequestConfig{
			{
				Marshaller: m,
				Messages:   []any{&wire1.Msg1{}},
			},
		},
	}

	client1, recvCh1, err := wave.NewClient(clientConfig)
	requireT.NoError(err)

	client2, recvCh2, err := wave.NewClient(clientConfig)
	requireT.NoError(err)

	client3, recvCh3, err := wave.NewClient(clientConfig3)
	requireT.NoError(err)

	group.Spawn("client1", parallel.Fail, client1.Run)
	group.Spawn("client2", parallel.Fail, client2.Run)
	group.Spawn("server", parallel.Fail, func(ctx context.Context) error {
		return wave.RunServer(ctx, ls, wave.ServerConfig{
			Servers:        servers,
			MaxMessageSize: maxMsgSize,
		})
	})

	requireT.NoError(client1.Send(&wire1.Msg1{
		Value: "test1",
	}, m))
	requireT.NoError(client2.Send(&wire1.Msg1{
		Value: "test2",
	}, m))

	group.Spawn("client3", parallel.Fail, client3.Run)

	testMsgs(ctx, requireT, recvCh1)
	testMsgs(ctx, requireT, recvCh2)
	testMsgs(ctx, requireT, recvCh3,
		&wire1.Msg1{Value: "test1"},
		&wire1.Msg1{Value: "test2"},
	)
}

func TestSameMessageReceivedTwiceIsIgnored(t *testing.T) {
	requireT := require.New(t)

	ctx := qa.NewContext(t)
	group := qa.NewGroup(ctx, t)

	defer func() {
		group.Exit(nil)
		requireT.NoError(group.Wait())
	}()

	ls1, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)
	ls2, err := net.Listen("tcp", "localhost:0")
	requireT.NoError(err)

	servers := []string{
		ls1.Addr().String(),
		ls2.Addr().String(),
	}

	m := wire1.NewMarshaller()
	clientConfig1 := wave.ClientConfig{
		Servers:        servers,
		MaxMessageSize: maxMsgSize,
	}
	clientConfig2 := wave.ClientConfig{
		Servers:        servers,
		MaxMessageSize: maxMsgSize,
		Requests: []wave.RequestConfig{
			{
				Marshaller: m,
				Messages:   []any{&wire1.Msg1{}},
			},
		},
	}

	client1, recvCh1, err := wave.NewClient(clientConfig1)
	requireT.NoError(err)

	client2, recvCh2, err := wave.NewClient(clientConfig2)
	requireT.NoError(err)

	group.Spawn("client1", parallel.Fail, client1.Run)
	group.Spawn("server", parallel.Fail, func(ctx context.Context) error {
		return wave.RunServer(ctx, ls1, wave.ServerConfig{
			Servers:        servers,
			MaxMessageSize: maxMsgSize,
		})
	})
	group.Spawn("server", parallel.Fail, func(ctx context.Context) error {
		return wave.RunServer(ctx, ls2, wave.ServerConfig{
			Servers:        servers,
			MaxMessageSize: maxMsgSize,
		})
	})

	group.Spawn("client2", parallel.Fail, client2.Run)

	requireT.NoError(client1.Send(&wire1.Msg1{
		Value: "test",
	}, m))

	testMsgs(ctx, requireT, recvCh1)
	testMsgs(ctx, requireT, recvCh2,
		&wire1.Msg1{Value: "test"},
	)
}

func testMsgs(ctx context.Context, requireT *require.Assertions, recvCh <-chan any, msgs ...any) {
	received := make([]any, 0, len(msgs))
	for range msgs {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
			requireT.Fail("timeout")
		case msg := <-recvCh:
			received = append(received, msg)
		}
	}

	requireT.ElementsMatch(msgs, received)
	requireT.Empty(recvCh)
}
