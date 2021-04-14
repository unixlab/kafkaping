package conf

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

type ConfigFlags struct {
	ClientName  string
	Brokers     []string
	SSL         bool
	CACertBytes []byte
	WaitTime    time.Duration
}

func (c ConfigFlags) GenerateSaramaConfig() *sarama.Config {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.ClientID = fmt.Sprintf("kafkaping-%s", c.ClientName)
	kafkaConfig.Producer.Retry.Max = 2
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Return.Successes = true
	return kafkaConfig
}
