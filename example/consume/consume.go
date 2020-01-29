package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/satriarrrrr/go-amqp-reconnect/rabbitmq"
)

func main() {
	errChan := make(chan error)

	rabbitmq.Debug = true
	conn, err := rabbitmq.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Println(">>> create client error: ", err)
		return
	}

	publishers := make([]*rabbitmq.Channel, 4)
	for i := 0; i < 4; i++ {
		ch, err := conn.Channel()
		if err != nil {
			log.Println(">>> create channel error: ", err)
			return
		}
		publishers[i] = ch
	}
	consumer, err := conn.Channel()
	if err != nil {
		log.Println(">>> create channel error: ", err)
		return
	}

	go func() {
		d, err := consumer.Consume("hello", "", false, false, false, false, nil)
		if err != nil {
			log.Panic(err)
		}

		for msg := range d {
			log.Printf(">>> msg: %s", string(msg.Body))
			msg.Ack(true)
		}
	}()

	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2)

		<-stop
		log.Println(">>> shutting down server...")

		errChan <- nil
	}()

	err = <-errChan
	if err != nil && err != http.ErrServerClosed {
		log.Println(">>> got error from errChan:", err)
	} else {
		log.Println(">>> server gracefully stopped")
	}

	consumer.Close()
	conn.Close()

	os.Exit(0)
}
