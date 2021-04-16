package server

import (
	"github.com/unixlab/kafkaping/conf"

	"github.com/Shopify/sarama"
)

func checkErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func Run(configFlags conf.ConfigFlags) {
	var err error

	var kafkaConsumer sarama.Consumer
	kafkaConsumer, err = sarama.NewConsumer(configFlags.Brokers, configFlags.GenerateSaramaConfig())
	checkErr(err)

	var pingConsumer sarama.PartitionConsumer
	pingConsumer, err = kafkaConsumer.ConsumePartition("ping", 0, sarama.OffsetNewest)
	checkErr(err)

	var kafkaProducer sarama.SyncProducer
	kafkaProducer, err = sarama.NewSyncProducer(configFlags.Brokers, configFlags.GenerateSaramaConfig())
	checkErr(err)

	defer func() {
		err = kafkaConsumer.Close()
		checkErr(err)
		err = kafkaProducer.Close()
		checkErr(err)
	}()

	for {
		msgRecv := <-pingConsumer.Messages()
		msgSend := &sarama.ProducerMessage{
			Topic: "pong",
			Value: sarama.StringEncoder(msgRecv.Value),
		}

		var partition int32
		partition, _, err = kafkaProducer.SendMessage(msgSend)
		checkErr(err)

		if partition != 0 {
			panic("pong topic has more than 1 partition")
		}
	}
}
