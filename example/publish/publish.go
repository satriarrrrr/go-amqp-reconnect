package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/satriarrrrr/go-amqp-reconnect/rabbitmq"
	"github.com/streadway/amqp"
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

	go func() {
		log.Println(">>> publisher is running...")
		for {
			for _, publisher := range publishers {
				go func(ch *rabbitmq.Channel) {
					uuid, _ := uuid.NewV4()
					err := ch.Publish(
						"",      // exchange
						"hello", // routing key
						false,   // mandatory
						false,   // immediate
						amqp.Publishing{
							MessageId:   uuid.String(),
							ContentType: "text/plain",
							Body:        []byte("Hello"),
						},
					)
					if err != nil {
						log.Println(">>> publish error:", err)
					} else {
						log.Println(">>> publish success")
					}
				}(publisher)
			}
			time.Sleep(1 * time.Second)
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

	publishers[0].Close()
	publishers[1].Close()
	publishers[2].Close()
	publishers[3].Close()
	conn.Close()

	os.Exit(0)
}
