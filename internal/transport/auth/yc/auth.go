package yc

import (
	"context"
	"errors"
	"time"

	ycsdk "github.com/yandex-cloud/go-sdk"
	"github.com/yandex-cloud/go-sdk/iamkey"
)

var (
	ErrIAMKeyUnspecified   = errors.New("either IAMKey or IAMKeyFile must be provided")
	ErrIAMKey              = errors.New("error reading IAM key")
	ErrIAMKeyFile          = errors.New("error reading IAM key file")
	ErrIAMTokenCreate      = errors.New("error creating IAM token")
	ErrServiceAccountCreds = errors.New("error reading service account credentials")
	ErrYCSDK               = errors.New("error reading YC SDK client")
)

type (
	// YC is Yandex Cloud authenticator.
	YC struct {
		sdk *ycsdk.SDK
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

func New(ctx context.Context, cfg Config) (*YC, error) {
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

	return &YC{sdk: yc}, nil
}

func (a *YC) GetToken(ctx context.Context) (token string, expires time.Time, err error) {
	tokenResp, err := a.sdk.CreateIAMToken(ctx)
	if err != nil {
		err = errors.Join(ErrIAMTokenCreate, err)

		return
	}

	token = tokenResp.GetIamToken()
	expires = tokenResp.GetExpiresAt().AsTime()

	return
}
