package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/unixlab/kafkaping/client"
	"github.com/unixlab/kafkaping/conf"
	"github.com/unixlab/kafkaping/server"
)

func printHelpAndExit() {
	fmt.Println("kafkaping (server|client) [-s ssl] [-b broker,broker,...] [-p port]")
	os.Exit(1)
}

func main() {
	var brokerList string
	var brokerPort int
	var sslEnabled bool

	if len(os.Args) < 2 {
		printHelpAndExit()
	}

	kafkaFlags := flag.NewFlagSet("",flag.ExitOnError)
	kafkaFlags.StringVar(&brokerList, "b", "127.0.0.1", "list of kafka brokers. comma separated")
	kafkaFlags.IntVar(&brokerPort, "p", 9092, "kafka port")
	kafkaFlags.BoolVar(&sslEnabled, "s", false, "use ssl")
	kafkaFlags.Parse(os.Args[2:])

	var kafkaConfig conf.Config

	for _, broker := range strings.Split(brokerList, ",") {
		kafkaConfig.Broker = append(kafkaConfig.Broker, fmt.Sprintf("%s:%d", broker, brokerPort))
	}

	kafkaConfig.SSL = sslEnabled

	switch os.Args[1] {
	case "server":
		server.Run(kafkaConfig)
	case "client":
		client.Run(kafkaConfig)
	default:
		printHelpAndExit()
	}
}
