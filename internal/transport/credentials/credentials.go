package credentials

import (
	"crypto/tls"
	"crypto/x509"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func Insecure() credentials.TransportCredentials {
	return insecure.NewCredentials()
}

func TLS() credentials.TransportCredentials {
	return credentials.NewTLS(tlsConfig())
}

func TLSSkipVerify() credentials.TransportCredentials {
	tlsCfg := tlsConfig()
	tlsCfg.InsecureSkipVerify = true
	return credentials.NewTLS(tlsCfg)
}

func tlsConfig() *tls.Config {
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    x509.NewCertPool(),
	}
	if sysPool, err := x509.SystemCertPool(); err == nil {
		tlsCfg.RootCAs = sysPool
	}
	return tlsCfg
}
