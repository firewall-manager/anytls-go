// Package session 实现了 AnyTLS 协议的会话和流管理功能。
package session

import (
	"anytls/proxy/padding"
	"anytls/util"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"sync"
	"time"

	"github.com/chen3feng/stl4go"
	"github.com/sagernet/sing/common/atomic"
	"github.com/sirupsen/logrus"
)

var clientDebugSessionPool = os.Getenv("CLIENT_DEBUG_SESSION_POOL") == "1"

// Client 管理客户端会话的创建、复用和清理。
// 支持会话池机制，可以复用空闲会话以提高性能。
type Client struct {
	die       context.Context
	dieCancel context.CancelFunc

	dialOut util.DialOutFunc

	sessionCounter atomic.Uint64

	idleSession     *stl4go.SkipList[uint64, *Session]
	idleSessionLock sync.Mutex

	sessions     map[uint64]*Session
	sessionsLock sync.Mutex

	padding *atomic.TypedValue[*padding.PaddingFactory]

	idleSessionTimeout time.Duration
	minIdleSession     int // 保持的最小空闲会话数
}

// NewClient 创建一个新的客户端会话管理器。
// ctx: 上下文，用于控制客户端生命周期
// dialOut: 用于创建新连接的函数
// _padding: 填充策略工厂
// idleSessionCheckInterval: 空闲会话检查间隔
// idleSessionTimeout: 空闲会话超时时间
// minIdleSession: 保持的最小空闲会话数
func NewClient(ctx context.Context, dialOut util.DialOutFunc,
	_padding *atomic.TypedValue[*padding.PaddingFactory], idleSessionCheckInterval, idleSessionTimeout time.Duration, minIdleSession int,
) *Client {
	c := &Client{
		sessions:           make(map[uint64]*Session),
		dialOut:            dialOut,
		padding:            _padding,
		idleSessionTimeout: idleSessionTimeout,
		minIdleSession:     minIdleSession,
	}
	if idleSessionCheckInterval <= time.Second*5 {
		idleSessionCheckInterval = time.Second * 30
	}
	if c.idleSessionTimeout <= time.Second*5 {
		c.idleSessionTimeout = time.Second * 30
	}
	c.die, c.dieCancel = context.WithCancel(ctx)
	c.idleSession = stl4go.NewSkipList[uint64, *Session]()
	util.StartRoutine(c.die, idleSessionCheckInterval, c.idleCleanup)
	return c
}

// CreateStream 创建一个新的流连接。
// 优先从空闲会话池中获取会话，如果没有则创建新会话。
func (c *Client) CreateStream(ctx context.Context) (net.Conn, error) {
	select {
	case <-c.die.Done():
		return nil, io.ErrClosedPipe
	default:
	}

	var session *Session
	var stream *Stream
	var err error

	session = c.getIdleSession()
	if session == nil {
		session, err = c.createSession(ctx)
		if session != nil && clientDebugSessionPool {
			logrus.Infoln("create session:", session.seq)
		}
	} else {
		if clientDebugSessionPool {
			logrus.Infoln("get session:", session.seq)
		}
	}
	if session == nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}
	stream, err = session.OpenStream()
	if err != nil {
		session.Close()
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	stream.dieHook = func() {
		// If Session is not closed, put this Stream to pool
		if !session.IsClosed() {
			if clientDebugSessionPool {
				logrus.Infoln("put session:", session.seq, stream.id)
			}
			select {
			case <-c.die.Done():
				// Now client has been closed
				go session.Close()
			default:
				c.idleSessionLock.Lock()
				session.idleSince = time.Now()
				c.idleSession.Insert(math.MaxUint64-session.seq, session)
				c.idleSessionLock.Unlock()
			}
		} else {
			if clientDebugSessionPool {
				logrus.Infoln("discard session stream:", session.seq, stream.id)
			}
		}
	}

	return stream, nil
}

// getIdleSession 从空闲会话池中获取一个会话。
func (c *Client) getIdleSession() (idle *Session) {
	c.idleSessionLock.Lock()
	if !c.idleSession.IsEmpty() {
		it := c.idleSession.Iterate()
		idle = it.Value()
		c.idleSession.Remove(it.Key())
	}
	c.idleSessionLock.Unlock()
	return
}

// createSession 创建一个新的会话并启动它。
func (c *Client) createSession(ctx context.Context) (*Session, error) {
	underlying, err := c.dialOut(ctx)
	if err != nil {
		return nil, err
	}

	session := NewClientSession(underlying, &padding.DefaultPaddingFactory)
	session.seq = c.sessionCounter.Add(1)
	session.dieHook = func() {
		if clientDebugSessionPool {
			logrus.Infoln("session died:", session.seq, session.streamId.Load(), session.pktCounter.Load())
		}

		c.idleSessionLock.Lock()
		c.idleSession.Remove(math.MaxUint64 - session.seq)
		c.idleSessionLock.Unlock()

		c.sessionsLock.Lock()
		delete(c.sessions, session.seq)
		c.sessionsLock.Unlock()
	}

	c.sessionsLock.Lock()
	c.sessions[session.seq] = session
	c.sessionsLock.Unlock()

	session.Run()
	return session, nil
}

// Close 关闭客户端，关闭所有会话。
func (c *Client) Close() error {
	c.dieCancel()

	c.sessionsLock.Lock()
	sessionToClose := make([]*Session, 0, len(c.sessions))
	for _, session := range c.sessions {
		sessionToClose = append(sessionToClose, session)
	}
	c.sessions = make(map[uint64]*Session)
	c.sessionsLock.Unlock()

	for _, session := range sessionToClose {
		session.Close()
	}

	return nil
}

// idleCleanup 清理超时的空闲会话。
func (c *Client) idleCleanup() {
	c.idleCleanupExpTime(time.Now().Add(-c.idleSessionTimeout))
}

// idleCleanupExpTime 清理在指定时间之前变为空闲的会话。
// 但至少保留 minIdleSession 个空闲会话。
func (c *Client) idleCleanupExpTime(expTime time.Time) {
	activeCount := 0
	var sessionToClose []*Session

	c.idleSessionLock.Lock()
	it := c.idleSession.Iterate()
	for it.IsNotEnd() {
		session := it.Value()
		key := it.Key()
		it.MoveToNext()

		if clientDebugSessionPool {
			logrus.Debugln("check session:", session.seq, expTime, session.idleSince)
		}

		if !session.idleSince.Before(expTime) {
			activeCount++
			continue
		}

		if activeCount < c.minIdleSession {
			session.idleSince = time.Now()
			activeCount++
			continue
		}

		sessionToClose = append(sessionToClose, session)
		c.idleSession.Remove(key)
	}
	c.idleSessionLock.Unlock()

	for _, session := range sessionToClose {
		if clientDebugSessionPool {
			logrus.Infoln("local cleanup session:", session.seq)
		}
		session.Close()
	}
}
