package main

import (
	"fmt"
	"os"

	"github.com/jamesainslie/gomq/config"
)

func main() {
	cfg := config.Default()
	fmt.Fprintf(os.Stderr, "gomq starting (data_dir=%s, amqp=%s:%d)\n",
		cfg.DataDir, cfg.AMQPBind, cfg.AMQPPort)
}
