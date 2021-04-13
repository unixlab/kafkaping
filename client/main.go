package client

import (
	"fmt"

	"github.com/unixlab/kafkaping/conf"
)

func Run(config conf.Config) {
	fmt.Println("I'm the client")
	fmt.Printf("config: %v\n", config)
}
