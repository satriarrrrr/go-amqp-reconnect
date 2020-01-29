package rabbitmq

import (
	"sync"
	"time"

	"sync/atomic"

	"github.com/streadway/amqp"
)

const (
	delay    = 1 // reconnect after delay seconds
	maxDelay = 5 // max delay seconds
)

// Connection amqp.Connection wrapper
type Connection struct {
	*amqp.Connection
	mu *sync.RWMutex
}

func (c *Connection) setConnection(conn *amqp.Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Connection = conn
}

func (c *Connection) getConnection() *amqp.Connection {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.Connection
}

// Channel wrap amqp.Connection.Channel, get a auto reconnect channel
func (c *Connection) Channel() (*Channel, error) {
	c.mu.RLock()
	ch, err := c.Connection.Channel()
	c.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	channel := &Channel{
		Channel: ch,
		mu:      &sync.RWMutex{},
	}

	go func() {
		for {
			reason, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || channel.IsClosed() {
				debug("channel closed")
				channel.Close() // close again, ensure closed flag set when connection closed
				break
			}
			debugf("channel closed, reason: %s\n", reason)

			// reconnect if not closed by developer
			counter := 1
			for {
				c.mu.RLock()
				ch, err := c.Connection.Channel()
				c.mu.RUnlock()
				if err == nil {
					debug("channel recreate success")
					channel.setChannel(ch)
					break
				}
				debugf("channel recreate failed, retrying in %d second(s) | err: %v \n", counter*delay, err)

				time.Sleep(time.Duration(counter*delay) * time.Second)
				counter = (counter + 1) % maxDelay
			}
		}

	}()

	return channel, nil
}

// Dial wrap amqp.Dial, dial and get a reconnect connection
func Dial(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	connection := &Connection{
		Connection: conn,
		mu:         &sync.RWMutex{},
	}

	go func() {
		for {
			connection.mu.RLock()
			reason, ok := <-connection.Connection.NotifyClose(make(chan *amqp.Error))
			connection.mu.RUnlock()
			// exit this goroutine if closed by developer
			if !ok {
				debug("connection closed")
				break
			}
			debugf("connection closed, reason: %v", reason)

			// reconnect if not closed by developer
			counter := 1
			for {
				conn, err := amqp.Dial(url)
				if err == nil {
					connection.setConnection(conn)
					debug("reconnect success")
					break
				}
				debugf("reconnect failed, retrying in %d second(s) | err: %v \n", counter*delay, err)

				time.Sleep(time.Duration(counter*delay) * time.Second)
				counter = (counter + 1) % maxDelay
			}
		}
	}()

	return connection, nil
}

// Channel amqp.Channel wapper
type Channel struct {
	*amqp.Channel
	closed int32
	mu     *sync.RWMutex
}

// IsClosed indicate closed by developer
func (ch *Channel) IsClosed() bool {
	return (atomic.LoadInt32(&ch.closed) == 1)
}

func (ch *Channel) setChannel(c *amqp.Channel) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.Channel = c
}

// Close ensure closed flag set
func (ch *Channel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&ch.closed, 1)

	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.Channel.Close()
}

// Consume wrap amqp.Channel.Consume, the returned delivery will end only when channel closed by developer
func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		for {
			ch.mu.RLock()
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			ch.mu.RUnlock()
			if err != nil {
				debugf("consume failed, err: %v", err)
				time.Sleep(delay * time.Second)
				continue
			}

			for msg := range d {
				deliveries <- msg
			}

			// sleep before IsClose call. closed flag may not set before sleep.
			time.Sleep(delay * time.Second)

			if ch.IsClosed() {
				break
			}
		}
	}()

	return deliveries, nil
}

// Publish wrap amqp.Channel.Publish
func (ch *Channel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	return ch.Channel.Publish(exchange, key, mandatory, immediate, msg)
}
