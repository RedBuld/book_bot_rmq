package book_bot_rmq

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RMQ_Session struct {
	params          *RMQ_Params
	logger          *log.Logger
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
}

type RMQ_Message struct {
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
	Params     amqp.Publishing
}

type RMQ_Params struct {
	Server   string               `json:"server" yaml:"server"`
	Queue    *RMQ_Params_Queue    `json:"queue" yaml:"queue"`
	Exchange *RMQ_Params_Exchange `json:"exchange" yaml:"exchange"`
	Prefetch *RMQ_Params_Prefetch `json:"prefetch" yaml:"prefetch"`
	Consumer func(amqp.Delivery)
}

type RMQ_Params_Queue struct {
	Name      string `json:"name" yaml:"name"`
	Durable   bool   `json:"durable" yaml:"durable"`
	Delete    bool   `json:"delete" yaml:"delete"`
	Exclusive bool   `json:"exclusive" yaml:"exclusive"`
	NoLocal   bool   `json:"no_local" yaml:"no_local"`
	NoWait    bool   `json:"no_wait" yaml:"no_wait"`
	AutoAck   bool   `json:"auto_ack" yaml:"auto_ack"`
}

type RMQ_Params_Prefetch struct {
	Count  int  `json:"count" yaml:"count"`
	Size   int  `json:"size" yaml:"size"`
	Global bool `json:"global" yaml:"global"`
}

type RMQ_Params_Exchange struct {
	Name       string `json:"name" yaml:"name"`
	Mode       string `json:"mode" yaml:"mode"`
	Durable    bool   `json:"durable" yaml:"durable"`
	Delete     bool   `json:"delete" yaml:"delete"`
	NoWait     bool   `json:"no_wait" yaml:"no_wait"`
	RoutingKey string `json:"routing_key" yaml:"routing_key"`
}

const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 2 * time.Second

	// When setting up the channel after a channel exception
	reInitDelay = 2 * time.Second

	// When resending messages the server didn't confirm
	resendDelay = 5 * time.Second
)

var (
	errNotConnected   = errors.New("not connected to a server")
	errAlreadyClosed  = errors.New("already closed: not connected to the server")
	errShutdown       = errors.New("session is shutting down")
	errNoQueueName    = errors.New("no queue name provided")
	errNoQueueParams  = errors.New("no queue params provided")
	errNoExchangeName = errors.New("no exchange name provided")
)

func NewRMQ(params *RMQ_Params) *RMQ_Session {
	session := RMQ_Session{
		params: params,
		logger: log.New(os.Stdout, "", log.LstdFlags),
		done:   make(chan bool),
	}
	session.logger.Println("RMQ starting connection")
	go session.handleReconnect()
	return &session
}

func (session *RMQ_Session) handleReconnect() {
	for {
		session.isReady = false
		session.logger.Println("RMQ attempting to connect")

		conn, err := session.connect()

		if err != nil {
			session.logger.Printf("RMQ Error: %+v\n", err)

			select {
			case <-session.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		if done := session.handleReInit(conn); done {
			break
		}
	}
}

func (session *RMQ_Session) connect() (*amqp.Connection, error) {
	conn, err := amqp.Dial(session.params.Server)

	if err != nil {
		return nil, err
	}

	session.changeConnection(conn)
	session.logger.Println("RMQ connected!")
	return conn, nil
}

func (session *RMQ_Session) handleReInit(conn *amqp.Connection) bool {
	for {
		session.isReady = false

		err := session.init(conn)

		if err != nil {
			session.logger.Printf("RMQ Error: %+v\n", err)

			select {
			case <-session.done:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}

		select {
		case <-session.done:
			return true
		case <-session.notifyConnClose:
			session.logger.Println("RMQ connection closed. Reconnecting...")
			return false
		case <-session.notifyChanClose:
			session.logger.Println("RMQ channel closed. Re-running init...")
		}
	}
}

func (session *RMQ_Session) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	session.logger.Println("RMQ channel created")

	err = ch.Confirm(false)
	if err != nil {
		return err
	}
	session.logger.Println("RMQ channel confirm set")

	if session.params.Queue != nil {
		if session.params.Queue.Name == "" {
			return errNoQueueName
		}
		_, err = ch.QueueDeclare(
			session.params.Queue.Name,      // name
			session.params.Queue.Durable,   // durable
			session.params.Queue.Delete,    // delete when unused
			session.params.Queue.Exclusive, // exclusive
			session.params.Queue.NoWait,    // no-wait
			nil,                            // arguments
		)
	} else {
		return errNoQueueParams
	}
	if err != nil {
		return err
	}
	session.logger.Println("RMQ queue declared")

	if session.params.Prefetch != nil {
		err = ch.Qos(
			session.params.Prefetch.Count,  // prefetch count
			session.params.Prefetch.Size,   // prefetch size
			session.params.Prefetch.Global, // global
		)
		if err != nil {
			return err
		}
		session.logger.Println("RMQ QoS set")
	}

	if session.params.Exchange != nil {
		if session.params.Exchange.Name == "" {
			return errNoExchangeName
		}
		if session.params.Exchange.Mode == "" {
			session.params.Exchange.Mode = "direct"
		}
		err = ch.ExchangeDeclare(
			session.params.Exchange.Name,    // name
			session.params.Exchange.Mode,    // type
			session.params.Exchange.Durable, // durable
			session.params.Exchange.Delete,  // auto-deleted
			false,                           // internal
			session.params.Exchange.NoWait,  // no-wait
			nil,                             // arguments
		)
		if err != nil {
			return err
		}
		session.logger.Println("RMQ " + session.params.Exchange.Name + " exchange declared")

		err = ch.QueueBind(
			session.params.Queue.Name,          // queue name
			session.params.Exchange.RoutingKey, // routing key
			session.params.Exchange.Name,       // exchange
			session.params.Queue.NoWait,        // no-wait
			nil,                                // arguments
		)
		if err != nil {
			panic(err)
		}
		session.logger.Println("RMQ binded queue to exchange " + session.params.Exchange.Name)
	}

	session.changeChannel(ch)
	session.isReady = true

	if session.params.Consumer != nil {
		for {
			messages, err := session.Stream()
			if err == nil {
				session.logger.Println("RMQ consumer ready")
				go func() {
					for message := range messages {
						session.params.Consumer(message)
					}
				}()
				break
			}
		}
	}

	session.logger.Println("RMQ setup!")

	return nil
}

func (session *RMQ_Session) changeConnection(connection *amqp.Connection) {
	session.connection = connection
	session.notifyConnClose = make(chan *amqp.Error)
	session.connection.NotifyClose(session.notifyConnClose)
}

func (session *RMQ_Session) changeChannel(channel *amqp.Channel) {
	session.channel = channel
	session.notifyChanClose = make(chan *amqp.Error)
	session.notifyConfirm = make(chan amqp.Confirmation, 1)
	session.channel.NotifyClose(session.notifyChanClose)
	session.channel.NotifyPublish(session.notifyConfirm)
}

func (session *RMQ_Session) Push(message *RMQ_Message) error {
	for {
		err := session.UnsafePush(message)
		if err != nil {
			session.logger.Println("Push failed. Retrying...")
			select {
			case <-session.done:
				return errShutdown
			case <-time.After(resendDelay):
			}
			continue
		}
		select {
		case confirm := <-session.notifyConfirm:
			if confirm.Ack {
				session.logger.Println("Push confirmed!")
				return nil
			}
		case <-time.After(resendDelay):
		}
		session.logger.Println("Push didn't confirm. Retrying...")
	}
}

func (session *RMQ_Session) UnsafePush(message *RMQ_Message) error {
	if !session.isReady {
		return errNotConnected
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return session.channel.PublishWithContext(
		ctx,
		message.Exchange,   // exchange
		message.RoutingKey, // routing key
		message.Mandatory,  // mandatory
		message.Immediate,  // immediate
		message.Params,
	)
}

func (session *RMQ_Session) Stream() (<-chan amqp.Delivery, error) {
	if !session.isReady {
		return nil, errNotConnected
	}
	return session.channel.Consume(
		session.params.Queue.Name,      // queue
		session.params.Queue.Name,      // consumer
		session.params.Queue.AutoAck,   // auto-ack
		session.params.Queue.Exclusive, // exclusive
		session.params.Queue.NoLocal,   // no-local
		session.params.Queue.NoWait,    // no-Wait
		nil,                            // args
	)
}

func (session *RMQ_Session) Close() error {
	if !session.isReady {
		return errAlreadyClosed
	}
	err := session.channel.Close()
	if err != nil {
		return err
	}
	err = session.connection.Close()
	if err != nil {
		return err
	}
	close(session.done)
	session.isReady = false
	return nil
}
