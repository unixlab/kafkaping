package client

import (
	"fmt"
	"strconv"
	"time"

	"github.com/unixlab/kafkaping/conf"

	"github.com/Shopify/sarama"
)

func checkErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func Run(configFlags conf.ConfigFlags) {
	kafkaConsumer, err := sarama.NewConsumer(configFlags.Brokers, configFlags.GenerateSaramaConfig())
	checkErr(err)

	pongConsumer, err := kafkaConsumer.ConsumePartition("pong", 0, sarama.OffsetNewest)
	checkErr(err)

	kafkaProducer, err := sarama.NewSyncProducer(configFlags.Brokers, configFlags.GenerateSaramaConfig())
	checkErr(err)

	defer func() {
		err = kafkaConsumer.Close()
		checkErr(err)
		err = kafkaProducer.Close()
		checkErr(err)
	}()

	go func() {
		for {
			msgSend := &sarama.ProducerMessage{
				Topic: "ping",
				Value: sarama.StringEncoder(strconv.FormatInt(time.Now().UnixNano(), 10)),
			}

			partition, _, err := kafkaProducer.SendMessage(msgSend)
			checkErr(err)

			if partition != 0 {
				panic("ping topic has more than 1 partition")
			}

			time.Sleep(configFlags.WaitTime)
		}
	}()

	for {
		msgRecv := <-pongConsumer.Messages()
		timestampStart, err := strconv.ParseInt(string(msgRecv.Value), 10, 64)
		if err != nil {
			continue
		}
		timestampEnd := time.Now().UnixNano()
		roundTripTime := timestampEnd - timestampStart

		duration, err := time.ParseDuration(strconv.FormatInt(roundTripTime, 10) + "ns")
		checkErr(err)

		fmt.Printf("%s\n", duration.String())
	}
}
