package conf

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

type ConfigFlags struct {
	Mode        string
	Brokers     []string
	SSL         bool
	CACertBytes []byte
	WaitTime    time.Duration
}

func (c ConfigFlags) GenerateSaramaConfig() *sarama.Config {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.ClientID = fmt.Sprintf("kafkaping-%s", c.Mode)
	kafkaConfig.Producer.Retry.Max = 2
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Return.Successes = true

	if c.SSL {
		tlsConfig := tls.Config{}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(c.CACertBytes)
		tlsConfig.RootCAs = caCertPool
		kafkaConfig.Net.TLS.Enable = true
		kafkaConfig.Net.TLS.Config = &tlsConfig
	}

	return kafkaConfig
}
