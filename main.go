package main

import (
	"crypto/x509"
	"flag"
	"fmt"
	"github.com/unixlab/kafkaping/conf"
	"github.com/unixlab/kafkaping/ping"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/unixlab/kafkaping/copy"
)

func checkErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func printHelp(kafkaFlags *flag.FlagSet) {
	fmt.Println("kafkaping (copy|ping|both) [-b broker,broker,...] [more Options]")
	fmt.Println()
	fmt.Println("Options:")
	kafkaFlags.PrintDefaults()
}

func main() {
	var brokerList string
	var brokerPort int
	var sslEnabled bool
	var caCertPath string
	var waitTime int
	var sourceTopic string
	var destinationTopic string
	var writeToTopic string
	var readFromTopic string

	kafkaFlags := flag.NewFlagSet("", flag.ExitOnError)
	kafkaFlags.StringVar(&brokerList, "b", "localhost", "list of kafka brokers. comma separated")
	kafkaFlags.IntVar(&brokerPort, "p", 9092, "kafka port")
	kafkaFlags.BoolVar(&sslEnabled, "s", false, "use ssl")
	kafkaFlags.StringVar(&caCertPath, "cacert", "", "CA certificate")
	kafkaFlags.IntVar(&waitTime, "w", 400, "wait time between messages in milliseconds")
	kafkaFlags.StringVar(&sourceTopic, "src", "ping", "kafka source topic (copy only)")
	kafkaFlags.StringVar(&destinationTopic, "dst", "pong", "kafka destination topic (copy only)")
	kafkaFlags.StringVar(&writeToTopic, "write-to", "ping", "kafka topic to write to (ping only)")
	kafkaFlags.StringVar(&readFromTopic, "read-from", "pong", "kafka topic to read from (ping only)")

	if len(os.Args) < 2 {
		printHelp(kafkaFlags)
		os.Exit(0)
	}

	err := kafkaFlags.Parse(os.Args[2:])
	checkErr(err)

	var kafkaConfig conf.ConfigFlags

	kafkaConfig.Mode = os.Args[1]
	kafkaConfig.WaitTime = time.Duration(waitTime) * time.Millisecond

	for _, broker := range strings.Split(brokerList, ",") {
		kafkaConfig.Brokers = append(kafkaConfig.Brokers, fmt.Sprintf("%s:%d", broker, brokerPort))
	}

	if caCertPath != "" && !sslEnabled {
		fmt.Println("implicitly enable ssl because --cacert is set")
		sslEnabled = true
	}

	kafkaConfig.SSL = sslEnabled

	if sslEnabled {
		if caCertPath == "" {
			fmt.Println("SSL is enabled, but --cacert is not set")
			os.Exit(0)
		}

		_, err = os.Stat(caCertPath)
		if os.IsNotExist(err) {
			fmt.Println("provided cacert could not be loaded")
			os.Exit(0)
		} else if err != nil {
			panic(err)
		}

		var caCertBytes []byte
		caCertBytes, err = ioutil.ReadFile(caCertPath)
		checkErr(err)

		// sanity check
		cacert := x509.NewCertPool()
		ok := cacert.AppendCertsFromPEM(caCertBytes)

		if !ok {
			fmt.Println("cacert file looks invalid")
			os.Exit(0)
		}

		kafkaConfig.CACertBytes = caCertBytes
	}

	switch os.Args[1] {
	case "copy":
		copy.Run(kafkaConfig, sourceTopic, destinationTopic)
	case "ping":
		ping.Run(kafkaConfig, readFromTopic, writeToTopic)
	case "both":
		kafkaConfig.Mode = "copy"
		go copy.Run(kafkaConfig, sourceTopic, destinationTopic)
		kafkaConfig.Mode = "ping"
		ping.Run(kafkaConfig, sourceTopic, destinationTopic)
	default:
		printHelp(kafkaFlags)
	}
}
