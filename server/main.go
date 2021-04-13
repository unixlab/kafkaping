package server

import (
	"fmt"

	"github.com/unixlab/kafkaping/conf"
)

func Run(config conf.Config) {
	fmt.Println("I'm the server")
	fmt.Printf("config: %v\n", config)
}
