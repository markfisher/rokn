package io

import (
	"github.com/streadway/amqp"
	"io"
	"log"
)

func CloseAmqpResourceAndExitOnError(closer io.Closer) {
	if err := closer.Close(); err != nil && err != amqp.ErrClosed {
		log.Fatal(err)
	}
}
