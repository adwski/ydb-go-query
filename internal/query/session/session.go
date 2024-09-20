package session

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adwski/ydb-go-query/internal/logger"
	"github.com/adwski/ydb-go-query/internal/xcontext"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	cleanupTimeout = 3 * time.Second
)

var (
	ErrSessionCreate    = errors.New("session create failed")
	ErrSessionTransport = errors.New("session transport was not provided")
	ErrSessionAttach    = errors.New("session attach failed")
	ErrSessionDelete    = errors.New("session delete failed")
)

var (
	hashSeed maphash.Seed
)

func init() {
	hashSeed = maphash.MakeSeed()
}

type (
	Session struct {
		logger logger.Logger

		transport grpc.ClientConnInterface

		qsc    Ydb_Query_V1.QueryServiceClient
		stream Ydb_Query_V1.QueryService_AttachSessionClient

		cancelFunc context.CancelFunc
		done       chan struct{}

		state *Ydb_Query.SessionState
		err   error
		id    string
		id_   uint64
		node  int64

		shutdown atomic.Bool
	}

	Config struct {
		Transport      grpc.ClientConnInterface
		CreateResponse *Ydb_Query.CreateSessionResponse
		Logger         logger.Logger
	}
)

func CreateSession(
	ctx context.Context,
	qsc Ydb_Query_V1.QueryServiceClient,
	logger logger.Logger,
	timeout time.Duration,
) (*Session, error) {
	var transport grpc.ClientConnInterface
	sessCtx := xcontext.WithTransportPtr(ctx, &transport)

	createCtx, cancel := context.WithTimeout(sessCtx, timeout)
	defer cancel()

	respCreate, err := qsc.CreateSession(createCtx, &Ydb_Query.CreateSessionRequest{})
	if err != nil {
		return nil, errors.Join(ErrSessionCreate, err)
	}
	if respCreate.Status != Ydb.StatusIds_SUCCESS {
		return nil, errors.Join(ErrSessionCreate, fmt.Errorf("status: %s", respCreate.Status))
	}

	if transport == nil {
		return nil, ErrSessionTransport
	}

	sess := &Session{
		logger:    logger,
		transport: transport,
		qsc:       Ydb_Query_V1.NewQueryServiceClient(transport),
		id:        respCreate.GetSessionId(),
		id_:       maphash.String(hashSeed, respCreate.GetSessionId()),
		node:      respCreate.GetNodeId(),
		done:      make(chan struct{}),
	}

	if err = sess.attachStream(ctx); err != nil {
		go func() { _ = sess.Close() }()
		return nil, err
	}

	return sess, nil
}

func (s *Session) ID() uint64 {
	return s.id_
}

func (s *Session) Alive() bool {
	return !s.shutdown.Load()
}

func (s *Session) Close() error {
	s.shutdown.Store(true)

	if s.cancelFunc != nil {
		// cancel stream
		s.cancelFunc()
	}

	// ensure stream is canceled
	<-s.done

	// cleanup session
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	err := errors.Join(s.err, s.cleanup(ctx))

	s.logger.Debug("session closed", "id", s.id)
	return err
}

func (s *Session) attachStream(ctx context.Context) error {
	attachCtx, streamCancel := context.WithCancel(ctx)

	respAttach, err := s.qsc.AttachSession(attachCtx, &Ydb_Query.AttachSessionRequest{
		SessionId: s.id,
	})
	if err != nil {
		streamCancel()
		s.err = err
		close(s.done)
		return errors.Join(ErrSessionAttach, err)
	}

	s.stream = respAttach
	s.cancelFunc = streamCancel

	s.logger.Trace("attached to session", "id", s.id, "node", s.node, "id_", s.id_)

	sig := make(chan struct{}) // async success signal
	go s.spin(sig)

	// Looks like attach mechanism is non-blocking.
	// AttachSession might finish but on YDB side
	// session still may be not attached for some short time.
	// Seems like transition to attached state is signaled by status:SUCCESS,
	// so we need to wait for status change before handling session to the pool.
	//
	// Otherwise, we in race condition and first query for this session may return BAD REQUEST.
	select {
	case <-sig:
	case <-s.done:
	}
	close(sig)

	return nil
}

func (s *Session) spin(sigSuccess chan<- struct{}) {
	once := sync.Once{}
	for {
		state, err := s.stream.Recv()
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				s.logger.Debug("session stream ended", "id", s.id)
			case status.Code(err) == codes.Canceled:
				s.logger.Trace("session stream context canceled", "id", s.id)
			default:
				s.logger.Error("session stream error", "id", s.id, "err", err)
				s.err = err
			}

			break
		}
		if s.state != state {
			// TODO: Check state (which states can we expect here?)
			s.logger.Debug("session state changed",
				"id", s.id, "node", s.node, "state", state)
			s.state = state
			if state.Status == Ydb.StatusIds_SUCCESS {
				once.Do(func() { sigSuccess <- struct{}{} })
			}
		}
	}
	s.shutdown.Store(true)
	close(s.done)
}

func (s *Session) cleanup(ctx context.Context) error {
	respDelete, err := s.qsc.DeleteSession(ctx, &Ydb_Query.DeleteSessionRequest{
		SessionId: s.id,
	})
	if err != nil {
		return errors.Join(ErrSessionDelete, err)
	}
	if respDelete.Status != Ydb.StatusIds_SUCCESS {
		return errors.Join(ErrSessionDelete,
			fmt.Errorf("status: %s", respDelete.Status))
	}

	return nil
}
