package main

import (
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/unixlab/kafkaping/client"
	"github.com/unixlab/kafkaping/conf"
	"github.com/unixlab/kafkaping/server"
)

func checkErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func printHelp() {
	fmt.Println("kafkaping (server|client|both) [-b broker,broker,...] [-p port] [-s ssl] [--cacapth path]")
}

func main() {
	var brokerList string
	var brokerPort int
	var sslEnabled bool
	var caCertPath string
	var waitTime int

	if len(os.Args) < 2 {
		printHelp()
		os.Exit(0)
	}

	kafkaFlags := flag.NewFlagSet("", flag.ExitOnError)
	kafkaFlags.StringVar(&brokerList, "b", "localhost", "list of kafka brokers. comma separated")
	kafkaFlags.IntVar(&brokerPort, "p", 9092, "kafka port")
	kafkaFlags.BoolVar(&sslEnabled, "s", false, "use ssl")
	kafkaFlags.StringVar(&caCertPath, "cacert", "", "CA certificate")
	kafkaFlags.IntVar(&waitTime, "w", 400, "wait time between messages in milliseconds")
	kafkaFlags.Parse(os.Args[2:])

	var kafkaConfig conf.ConfigFlags

	kafkaConfig.ClientName = os.Args[1]
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

		_, err := os.Stat(caCertPath)
		if os.IsNotExist(err) {
			fmt.Println("provided cacert could not be loaded")
			os.Exit(0)
		} else if err != nil {
			panic(err)
		}

		caCertBytes, err := ioutil.ReadFile(caCertPath)
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
	case "server":
		server.Run(kafkaConfig)
	case "client":
		client.Run(kafkaConfig)
	default:
		printHelp()
	}
}
