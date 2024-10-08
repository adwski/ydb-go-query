package auth

import (
	"context"
	"sync"
	"time"

	"github.com/adwski/ydb-go-query/internal/logger"
)

const (
	defaultTokenCallTimeout       = 5 * time.Second
	defaultTokenRenewFailInterval = 10 * time.Second
	defaultTokenInitialRetry      = time.Second
)

type (
	Provider interface {
		GetToken(ctx context.Context) (string, time.Time, error)
	}
	Auth struct {
		logger   logger.Logger
		provider Provider

		mx *sync.RWMutex

		timer *time.Timer

		expires time.Time

		token string

		renewDisable bool
	}
	Config struct {
		Provider     Provider
		Logger       logger.Logger
		RenewDisable bool
	}
)

func New(ctx context.Context, cfg Config) (*Auth, error) {
	auth := &Auth{
		provider:     cfg.Provider,
		logger:       cfg.Logger,
		renewDisable: cfg.RenewDisable,

		mx: &sync.RWMutex{},
	}

	return auth, auth.mustGetToken(ctx)
}

func (a *Auth) GetToken() string {
	a.mx.RLock()
	defer a.mx.RUnlock()

	return a.token
}

func (a *Auth) mustGetToken(ctx context.Context) (err error) {
getTokenLoop:
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err = a.getTokenTick(ctx); err == nil {
				break getTokenLoop
			}
			time.Sleep(defaultTokenInitialRetry)
		}
	}

	return //nolint:nilerr // unnecessary
}

func (a *Auth) getTokenTick(ctx context.Context) error {
	ctxCall, cancel := context.WithTimeout(ctx, defaultTokenCallTimeout)
	defer cancel()

	token, expires, err := a.provider.GetToken(ctxCall)
	if err != nil {
		a.logger.Error("token error", "error", err)
		a.setTimer(defaultTokenRenewFailInterval)

		return err //nolint:wrapcheck //unnecessary
	}

	a.mx.Lock()
	a.token = token
	a.expires = expires
	a.mx.Unlock()

	renew := a.expires.Sub(time.Now().UTC()) / 2
	a.setTimer(renew)

	a.logger.Info("token retrieved successfully",
		"expiresAt", a.expires.Format(time.RFC3339),
		"renewIn", renew.Truncate(time.Second))

	return nil
}

func (a *Auth) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	if a.renewDisable {
		return
	}

	a.logger.Debug("auth token renew started")
	defer func() {
		a.timer.Stop()
		a.logger.Debug("auth token renew stopped")
	}()

renewLoop:
	for {
		select {
		case <-ctx.Done():
			break renewLoop
		case <-a.timer.C:
			_ = a.getTokenTick(ctx)
		}
	}
}

func (a *Auth) setTimer(dur time.Duration) {
	if a.timer == nil {
		a.timer = time.NewTimer(dur)
	} else {
		a.timer.Reset(dur)
	}
}
