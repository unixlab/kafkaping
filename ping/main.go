package ping

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

func Run(configFlags conf.ConfigFlags, readFromTopic string, writeToTopic string) {
	var err error

	var kafkaConsumer sarama.Consumer
	kafkaConsumer, err = sarama.NewConsumer(configFlags.Brokers, configFlags.GenerateSaramaConfig())
	checkErr(err)

	var pongConsumer sarama.PartitionConsumer
	pongConsumer, err = kafkaConsumer.ConsumePartition(readFromTopic, 0, sarama.OffsetNewest)
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

	go func() {
		for {
			msgSend := &sarama.ProducerMessage{
				Topic: writeToTopic,
				Value: sarama.StringEncoder(strconv.FormatInt(time.Now().UnixNano(), 10)),
			}

			var partition int32
			partition, _, err = kafkaProducer.SendMessage(msgSend)
			checkErr(err)

			if partition != 0 {
				panic(fmt.Sprintf("topic %s has more than 1 partition", writeToTopic))
			}

			time.Sleep(configFlags.WaitTime)
		}
	}()

	for {
		var timestampStart, timestampEnd int64

		msgRecv := <-pongConsumer.Messages()

		timestampStart, err = strconv.ParseInt(string(msgRecv.Value), 10, 64)
		if err != nil {
			continue
		}

		timestampEnd = time.Now().UnixNano()

		roundTripTime := timestampEnd - timestampStart

		var duration time.Duration
		duration, err = time.ParseDuration(strconv.FormatInt(roundTripTime, 10) + "ns")
		checkErr(err)

		fmt.Printf("%s\n", duration.String())
	}
}
