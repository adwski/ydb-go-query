package userpass

import (
	"context"
	"errors"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Auth_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Auth"
	"google.golang.org/grpc"
)

const (
	defaultYDBAuthExpire = 12 * time.Hour
)

var (
	ErrLogin           = errors.New("login request failed")
	ErrNilOperation    = errors.New("nil operation")
	ErrUnauthorized    = errors.New("unauthorized")
	ErrLoginUnmarshall = errors.New("login response unmarshall failed")
)

type (
	UserPass struct {
		authSvc Ydb_Auth_V1.AuthServiceClient

		user string
		pass string
	}
	Config struct {
		Transport grpc.ClientConnInterface
		Username  string
		Password  string
	}
)

func New(cfg Config) *UserPass {
	auth := &UserPass{
		authSvc: Ydb_Auth_V1.NewAuthServiceClient(cfg.Transport),

		user: cfg.Username,
		pass: cfg.Password,
	}

	return auth
}

func (up *UserPass) GetToken(ctx context.Context) (token string, expires time.Time, err error) {
	resp, err := up.authSvc.Login(ctx, &Ydb_Auth.LoginRequest{
		User:     up.user,
		Password: up.pass,
	})

	if err != nil {
		err = errors.Join(ErrLogin, err)
		return
	}
	op := resp.GetOperation()
	if op == nil {
		err = errors.Join(ErrLogin, ErrNilOperation)
		return
	}
	if op.GetStatus() == Ydb.StatusIds_UNAUTHORIZED {
		err = ErrUnauthorized
		return
	}
	var result Ydb_Auth.LoginResult
	if err = op.GetResult().UnmarshalTo(&result); err != nil {
		err = errors.Join(ErrLoginUnmarshall, err)
		return
	}

	token = result.Token
	expires = time.Now().Add(defaultYDBAuthExpire)

	return
}
