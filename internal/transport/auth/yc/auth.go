package yc

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/adwski/ydb-go-query/v1/internal/logger"

	ycsdk "github.com/yandex-cloud/go-sdk"
	"github.com/yandex-cloud/go-sdk/iamkey"
)

const (
	defaultTokenCallTimeout       = 5 * time.Second
	defaultTokenRenewFailInterval = 10 * time.Second
)

var (
	ErrIAMKeyUnspecified   = errors.New("either IAMKey or IAMKeyFile must be provided")
	ErrIAMKey              = errors.New("error reading IAM key")
	ErrIAMKeyFile          = errors.New("error reading IAM key file")
	ErrServiceAccountCreds = errors.New("error reading service account credentials")
	ErrYCSDK               = errors.New("error reading YC SDK client")
)

type (
	// YC is Yandex Cloud authenticator.
	YC struct {
		logger  logger.Logger
		mx      *sync.RWMutex
		yc      *ycsdk.SDK
		timer   *time.Timer
		err     error
		expires time.Time
		token   string
		authInd bool
	}

	// Config is Yandex CLoud authenticator config.
	Config struct {
		// IamKeyFile specifies file path to IAM key file in jsom format.
		IamKeyFile string

		// IamKey specifies service account IAM key (usually in json format).
		// This param (if not empty) takes precedence over IamKeyFile.
		IamKey []byte
	}
)

func (a *YC) Ok() bool {
	return a.token != "" && a.err == nil
}

func (a *YC) GetToken() string {
	a.mx.RLock()
	defer a.mx.RUnlock()

	return a.token
}

func (a *YC) ExpiresAtUTC() time.Time {
	a.mx.RLock()
	defer a.mx.RUnlock()

	return a.expires
}

func New(ctx context.Context, log logger.Logger, cfg Config) (*YC, error) {
	if cfg.IamKeyFile == "" && len(cfg.IamKey) == 0 {
		return nil, ErrIAMKeyUnspecified
	}
	var (
		key *iamkey.Key
		err error
	)
	if len(cfg.IamKey) > 0 {
		if key, err = iamkey.ReadFromJSONBytes(cfg.IamKey); err != nil {
			return nil, errors.Join(ErrIAMKey, err)
		}
	} else {
		if key, err = iamkey.ReadFromJSONFile(cfg.IamKeyFile); err != nil {
			return nil, errors.Join(ErrIAMKeyFile, err)
		}
	}

	creds, err := ycsdk.ServiceAccountKey(key)
	if err != nil {
		return nil, errors.Join(ErrServiceAccountCreds, err)
	}

	yc, err := ycsdk.Build(ctx, ycsdk.Config{ // seems like Build() doesn't use context
		Credentials: creds,
	})
	if err != nil {
		return nil, errors.Join(ErrYCSDK, err)
	}

	ycAuth := &YC{
		logger: log,
		yc:     yc,
		mx:     &sync.RWMutex{},
	}

	// need to call it here once to initialize timer
	ycAuth.getToken(ctx)

	return ycAuth, nil
}

func (a *YC) Run(ctx context.Context, wg *sync.WaitGroup) {
	a.logger.Debug("auth renew started")
	defer func() {
		a.timer.Stop()
		wg.Done()
		a.logger.Debug("auth renew stopped")
	}()

renewTokenLoop:
	for {
		select {
		case <-ctx.Done():
			break renewTokenLoop
		case <-a.timer.C:
			a.getToken(ctx)
		}
	}
}

func (a *YC) getToken(ctx context.Context) {
	callCtx, cancel := context.WithTimeout(ctx, defaultTokenCallTimeout)
	defer cancel()

	tokenResp, err := a.yc.CreateIAMToken(callCtx)
	if err != nil {
		if a.authInd || a.err == nil || a.err.Error() != err.Error() {
			a.logger.Error("unable to authenticate in YC", "error", err)
			a.authInd = false
		}
		a.err = err
		a.setTimer(defaultTokenRenewFailInterval)
		return
	} else {
		a.err = nil
	}

	a.mx.Lock()
	a.token = tokenResp.IamToken
	a.expires = tokenResp.ExpiresAt.AsTime()
	a.mx.Unlock()

	// renew after half of expiration period
	renew := a.expires.Sub(time.Now().UTC()) / 2

	a.logger.Info("successfully authenticated in YC",
		"expiresAt", a.expires.Format(time.RFC3339),
		"renewIn", renew.Truncate(time.Second))
	a.authInd = true

	// set timer
	a.setTimer(renew)
}

func (a *YC) setTimer(dur time.Duration) {
	if a.timer == nil {
		a.timer = time.NewTimer(dur)
	} else {
		a.timer.Reset(dur)
	}
}
