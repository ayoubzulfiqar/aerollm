package mesh

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
)

// MDNSDiscoveryTag is the mDNS service tag for AeroLLM mesh peers.
const MDNSDiscoveryTag = "aerollm-mesh"

// libp2pTransport wraps libp2p host, streams, and discovery.
type libp2pTransport struct {
	mu       sync.RWMutex
	host     host.Host
	listener network.Listener
	peers    map[peer.ID]PeerDescriptor
	connCh   chan PeerConn
	closed   bool
}

// NewLibp2pTransport creates a new libp2p-backed transport.
func NewLibp2pTransport(ctx context.Context, listenAddr string) (*libp2pTransport, error) {
	h, err := libp2p.New(
		libp2p.ListenAddrStrings(listenAddr),
		libp2p.Ping(true),
	)
	if err != nil {
		return nil, fmt.Errorf("libp2p new: %w", err)
	}

	lst, err := h.Network().Listen(context.Background())
	if err != nil {
		return nil, fmt.Errorf("libp2p listen: %w", err)
	}

	t := &libp2pTransport{
		host:     h,
		listener: lst,
		peers:    make(map[peer.ID]PeerDescriptor),
		connCh:   make(chan PeerConn, 64),
	}

	if err := mdns.NewMdnsService(h, MDNSDiscoveryTag, &mdnsNotifier{t: t}); err != nil {
		return nil, fmt.Errorf("mdns init: %w", err)
	}

	go t.acceptLoop(ctx)

	return t, nil
}

// Dial opens a stream to a peer.
func (t *libp2pTransport) Dial(ctx context.Context, peerDesc PeerDescriptor) (PeerConn, error) {
	info, err := peer.AddrInfoFromString(peerDesc.Address)
	if err != nil {
		return nil, fmt.Errorf("bad peer address: %w", err)
	}
	if err := t.host.Connect(ctx, *info); err != nil {
		return nil, fmt.Errorf("connect peer: %w", err)
	}
	stream, err := t.host.NewStream(ctx, info.ID, MDNSDiscoveryTag)
	if err != nil {
		return nil, fmt.Errorf("open stream: %w", err)
	}
	conn := &libp2pConn{stream: stream, peer: peerDesc.ID}
	return conn, nil
}

// Listen advertises the local listener.
func (t *libp2pTransport) Listen(ctx context.Context, address string) (PeerListener, error) {
	return &libp2pListener{t: t}, nil
}

// Close tears down the transport.
func (t *libp2pTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	close(t.connCh)
	return t.host.Close()
}

// Peers returns known peer descriptors.
func (t *libp2pTransport) Peers() []PeerDescriptor {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]PeerDescriptor, 0, len(t.peers))
	for _, p := range t.peers {
		out = append(out, p)
	}
	return out
}

func (t *libp2pTransport) acceptLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case stream, ok := <-t.host.Network().Peers():
			if !ok {
				return
			}
			_ = stream
		default:
			stream, err := t.listener.Accept()
			if err != nil {
				if t.mu.RLock(); !t.closed; t.mu.RUnlock() {
					time.Sleep(100 * time.Millisecond)
				}
				continue
			}
			remotePeer := stream.Conn().RemotePeer()
			conn := &libp2pConn{stream: stream, peer: PeerID(remotePeer.String())}
			select {
			case t.connCh <- conn:
			default:
				_ = conn.Close()
			}
		}
	}
}

type libp2pConn struct {
	stream network.Stream
	peer   PeerID
	mu     sync.Mutex
	closed bool
}

func (c *libp2pConn) Send(_ context.Context, env Envelope) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return io.EOF
	}
	_ = env
	_, err := c.stream.Write([]byte("ping\n"))
	if err != nil {
		return err
	}
	return nil
}

func (c *libp2pConn) Receive(_ context.Context) (<-chan Envelope, error) {
	ch := make(chan Envelope, 8)
	go func() {
		defer close(ch)
		scanner := bufio.NewScanner(c.stream)
		for scanner.Scan() {
			ch <- Envelope{From: c.peer, Payload: scanner.Bytes()}
		}
	}()
	return ch, nil
}

func (c *libp2pConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.stream.Close()
}

type libp2pListener struct {
	t *libp2pTransport
}

func (l *libp2pListener) Accept(ctx context.Context) (PeerConn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case conn, ok := <-l.t.connCh:
		if !ok {
			return nil, io.EOF
		}
		return conn, nil
	}
}

func (l *libp2pListener) Close() error { return nil }

type mdnsNotifier struct {
	t *libp2pTransport
}

func (n *mdnsNotifier) HandlePeerFound(p peer.AddrInfo) {
	_ = p
}
